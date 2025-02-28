import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone, timedelta
from typing import Final, Any, Dict, List, DefaultDict

import aiohttp
from aiochclient import ChClient
from aiohttp import ClientSession, ClientResponse

from settings import get_settings

"""
В основе параллелизма лежит очередь тасков - _task_queue.

Таски могут быть двух типов: _process_commit_page и _process_watchers.
_process_commit_page запрашивает и парсит страницу с коммитами и, если она первая, 
создает оставшиеся N-1 аналогичных тасков, по одному на страницу коммитов в репе, и кладет их в очередь.
_process_watchers запрашивает и парсит количество watcher'ов у репы. Это нужно делать потому что в ответе search/repositories 
в поле с названием watchers вместо нужного значения хранится количество звезд, а поля subscribers_count, в котором
лежит нужное значение, нет.
(https://docs.github.com/en/rest/activity/starring?apiVersion=2022-11-28#starring-versus-watching).

Количество одновременных запросов регулируется количеством worker'ов, обслуживающих очередь.
Количество запросов в секунду - методом _rate_limiter().
Как только вся информация для репозитория собрана, он отправляется в БД.
"""


logging.basicConfig(level=logging.INFO)

GITHUB_API_BASE_URL: Final[str] = "https://api.github.com"
SETTINGS = get_settings()


@dataclass
class RepositoryAuthorCommitsNum:
    author: str
    commits_num: int


@dataclass
class Repository:
    name: str
    owner: str
    position: int
    stars: int
    watchers: int
    forks: int
    language: str
    authors_commits_num_today: list[RepositoryAuthorCommitsNum]


@dataclass
class TempRepository(Repository):
    pages_left: int = 1
    watchers_done: bool = False
    authors_dict: DefaultDict[str, int] = field(default_factory=lambda: defaultdict(int))


@dataclass
class CommitPageTask:
    owner: str
    repo: str
    since: str
    until: str
    page: int


@dataclass
class WatchersTask:
    owner: str
    repo: str


class GithubReposScrapper:
    def __init__(self, access_token: str, rps: int, concurrent_limit: int):
        self._session = None
        self._db_session = None
        self._db_client = None
        self._exec_time = None
        self._access_token = access_token
        self._task_queue = asyncio.Queue()
        self._temp_repos_result: Dict[str, TempRepository] = {}
        self._repos_result: List[Repository] = []

        self._concurrent_limit = concurrent_limit
        self._rps = rps
        self._rate_limit_tokens = rps
        self._last_refill = time.perf_counter_ns()

        self._amount_of_requests = 0
        self._start = None
        self._last_print = None

    async def _check_repo_finished(self, repo: str) -> None:
        conditions = [
            self._temp_repos_result[repo].pages_left == 0,
            self._temp_repos_result[repo].watchers_done
        ]
        if all(conditions):
            filled_repo = self._temp_repos_result.pop(repo)
            finished_repo = Repository(
                name=filled_repo.name,
                owner=filled_repo.owner,
                position=filled_repo.position,
                stars=filled_repo.stars,
                watchers=filled_repo.watchers,
                forks=filled_repo.forks,
                language=filled_repo.language,
                authors_commits_num_today=[
                    RepositoryAuthorCommitsNum(
                        author=author,
                        commits_num=commits_num
                    ) for author, commits_num in filled_repo.authors_dict.items()
                ]
            )
            self._repos_result.append(finished_repo)
            await self._insert_into_ch(finished_repo)

    async def _insert_into_ch(self, repo: Repository) -> None:
        day = self._exec_time.replace(hour=0, minute=0, second=0, microsecond=0) - timedelta(days=1)
        await asyncio.gather(
            self._db_client.execute(
                'INSERT INTO test.repositories (name, owner, stars, watchers, forks, language, updated) VALUES',
                (repo.name, repo.owner, repo.stars, repo.watchers, repo.forks, repo.language,
                 self._exec_time.replace(microsecond=0))
            ),
            self._db_client.execute(
                'INSERT INTO test.repositories_positions (date, repo, position) VALUES',
                (day, repo.name, repo.position)
            ),
            self._db_client.execute(
                'INSERT INTO test.repositories_authors_commits (date, repo, author, commits_num) VALUES',
                *[(day, repo.name, author.author, author.commits_num) for author in repo.authors_commits_num_today]
            )
        )

    def _rate_limiter(self):
        tick = 1_000_000_000 // self._rps
        now = time.perf_counter_ns()
        elapsed = now - self._last_refill
        if elapsed > tick:
            self._rate_limit_tokens = min(self._rps, self._rate_limit_tokens + elapsed // tick)
            self._last_refill = now
        if self._rate_limit_tokens > 0:
            self._rate_limit_tokens -= 1
            return True
        else:
            return False

    async def _make_request(self, endpoint: str, method: str = "GET",
                            params: dict[str, Any] | None = None) -> ClientResponse:
        while True:
            if self._rate_limiter():
                break
            else:
                await asyncio.sleep(1 // self._rps)

        self._amount_of_requests += 1
        now = time.perf_counter()
        if now - self._last_print > 0.3:
            logging.info(
                f'Requests per second: {self._amount_of_requests / (now - self._start)}, '
                f'total requests: {self._amount_of_requests}, '
                f'time spent: {round(now - self._start, 2)} s')
            self._last_print = now

        async with self._session.request(method, f"{GITHUB_API_BASE_URL}/{endpoint}", params=params) as response:
            if response.status != 200:
                text = await response.text()
                raise aiohttp.ClientResponseError(
                    status=response.status,
                    message=f'Unexpected status: {response.status} - {text}',
                    request_info=response.request_info,
                    history=response.history
                )
            else:
                await response.read()
                return response

    async def _get_top_repositories(self, limit: int = 100) -> ClientResponse:
        """GitHub REST API: https://docs.github.com/en/rest/search/search?apiVersion=2022-11-28#search-repositories"""
        return await self._make_request(
            endpoint="search/repositories",
            params={"q": "stars:>1", "sort": "stars", "order": "desc", "per_page": limit},
        )

    async def _get_watchers(self, owner: str, repo: str, per_page: int = 100) -> ClientResponse:
        return await self._make_request(
            endpoint=f"repos/{owner}/{repo}/subscribers",
            params={'per_page': per_page},
        )

    async def _get_repository_commits(self, owner: str, repo: str, since: str, until: str,
                                      page: int = 1) -> ClientResponse:
        """GitHub REST API: https://docs.github.com/en/rest/commits/commits?apiVersion=2022-11-28#list-commits"""
        return await self._make_request(
            endpoint=f"repos/{owner}/{repo}/commits",
            params={"per_page": 100, 'since': since, 'until': until, 'page': page},
        )

    async def _process_get_repos(self) -> None:
        response = await self._get_top_repositories()
        data = await response.json()
        repos = data["items"]
        beginning_of_today = self._exec_time.replace(hour=0, minute=0, second=0, microsecond=0)
        beginning_of_yesterday = beginning_of_today - timedelta(days=1)
        since = beginning_of_yesterday.strftime("%Y-%m-%dT%H:%M:%SZ")
        until = beginning_of_today.strftime("%Y-%m-%dT%H:%M:%SZ")
        for pos, repo in enumerate(repos):
            if repo['name'] not in self._temp_repos_result:
                self._temp_repos_result[repo['name']] = TempRepository(
                    name=repo['name'],
                    owner=repo['owner']['login'],
                    position=pos,
                    stars=repo['watchers'],
                    watchers=0,
                    forks=repo['forks_count'],
                    language=repo['language'],
                    authors_commits_num_today=[]
                )
            await self._task_queue.put(
                self._process_watchers(
                    WatchersTask(
                        owner=repo['owner']['login'],
                        repo=repo['name']
                    )
                )
            )
            await self._task_queue.put(
                self._process_commit_page(
                    CommitPageTask(
                        owner=repo['owner']['login'],
                        repo=repo['name'],
                        since=since,
                        until=until,
                        page=1
                    )
                )
            )

    async def _process_commit_page(self, params: CommitPageTask) -> None:
        response = await self._get_repository_commits(
            owner=params.owner,
            repo=params.repo,
            since=params.since,
            until=params.until,
            page=params.page
        )
        cur_repo = self._temp_repos_result[params.repo]
        if params.page == 1:
            last_page = 1
            if 'last' in response.links:
                last_page = int(response.links['last']['url'].query.get('page'))
                cur_repo.pages_left = last_page
            for page in range(2, last_page + 1):
                await self._task_queue.put(
                    self._process_commit_page(
                        CommitPageTask(
                            owner=params.owner,
                            repo=params.repo,
                            since=params.since,
                            until=params.until,
                            page=page
                        )
                    )
                )

        data = await response.json()
        for commit in data:
            author = (commit.get('commit') or {}).get('author', {}).get('name', '')
            if author:
                cur_repo.authors_dict[author] += 1
        cur_repo.pages_left -= 1
        await self._check_repo_finished(params.repo)

    async def _process_watchers(self, params: WatchersTask) -> None:
        response = await self._get_watchers(
            owner=params.owner,
            repo=params.repo,
            per_page=1
        )
        if 'last' in response.links:
            watchers = int(response.links['last']['url'].query.get('page'))
        else:
            data = await response.json()
            watchers = len(data)
        self._temp_repos_result[params.repo].watchers = watchers
        self._temp_repos_result[params.repo].watchers_done = True
        await self._check_repo_finished(params.repo)

    async def _worker(self) -> None:
        while True:
            task = await self._task_queue.get()
            if task is None:
                break
            task_name = task.__qualname__
            task_params = task.cr_frame.f_locals['params']
            try:
                await task
            except Exception:
                logging.exception(f'Error in {task_name} with params {task_params}.')
            finally:
                self._task_queue.task_done()

    async def get_repositories(self) -> list[Repository]:
        self._session = ClientSession(
            headers={
                "Accept": "application/vnd.github.v3+json",
                "Authorization": f"Bearer {self._access_token}",
            }
        )
        self._db_session = ClientSession()
        self._db_client = ChClient(
            session=self._db_session,
            url=SETTINGS.ch_url,
            database=SETTINGS.ch_db,
            user=SETTINGS.ch_user,
            password=SETTINGS.ch_password,
        )
        self._start = time.perf_counter()
        self._last_print = time.perf_counter()
        self._exec_time = datetime.now(timezone.utc)

        try:
            await self._process_get_repos()
            workers = [asyncio.create_task(self._worker()) for _ in range(self._concurrent_limit)]
            await self._task_queue.join()
            for _ in range(self._concurrent_limit):
                await self._task_queue.put(None)
            await asyncio.gather(*workers)
        except Exception as e:
            logging.exception(e)

        await self._db_client.close()
        await self._db_session.close()
        await self._session.close()
        return self._repos_result


async def main():
    scrapper = GithubReposScrapper(
        access_token=SETTINGS.github_token,
        rps=20,
        concurrent_limit=20
    )
    await scrapper.get_repositories()


asyncio.run(main())
