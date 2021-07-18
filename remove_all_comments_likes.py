import asyncio
import argparse
import random
import re
import os

import aiovk
from aiovk.exceptions import VkAPIError
from bs4 import BeautifulSoup


TOKEN = "token"
COMMENTS = "comments_path"
LIKES = "likes_path"
TIMEOUT = "timeout"
RANDOM_TIMEOUT = "random_timeout"
MAX_TASKS = "max_tasks"


OBJECTS_TYPES = {COMMENTS: "Comment", LIKES: "Like"}


class VkErrorInfo:
    error = {
        15: "Доступ запрещён. Или лайк уже убран.",
        30: "Профиль является приватным.",
        100: "Один из необходимых параметров был неверен.(Объект не существует)",
        211: "Нет доступа к комментариям."
    }


class VkDeleter:
    api: aiovk.API

    def __init__(self, access_token: str, paths: dict, timeout: float, max_tasks: int, random_timeout: tuple=None):
        self.paths: dict = paths
        self.access_token: str = access_token
        self.timeout: float = timeout
        self.max_tasks = max_tasks
        self.random_timeout = random_timeout

        self.counters = {"total": [0, 0]}
        for item in paths.keys():
            self.counters[item] = [0, 0]

        self._caller = {
            COMMENTS: [self.get_comment_credentials, self.delete_comment],
            LIKES: [self.get_likes_credentials, self.delete_like]
        }

    def get_hrefs(self, data_path):
        hrefs = []
        for filename in os.listdir(data_path):
            with open(f"{data_path}/{filename}", "r", encoding="ISO-8859-1") as file:
                val = file.read()
                links = BeautifulSoup(val, "lxml").find_all("div", {"class": "item"})
                hrefs.extend([link.a["href"] for link in links])
        return hrefs

    def get_comment_credentials(self):
        posts = self.get_hrefs(self.paths[COMMENTS])
        self.counters[COMMENTS][1] = len(posts)
        join_posts = "".join(posts)
        walls = re.findall(r"wall([-]?\d*)", join_posts)
        replies = re.findall(r"reply=(\d*)", join_posts)
        return zip(walls, replies, posts)

    def get_likes_credentials(self):
        posts = self.get_hrefs(self.paths[LIKES])
        self.counters[LIKES][1] = len(posts)
        join_posts = "".join(posts)
        type_ = re.findall(r"/([A-Za-z]+)-?\d+_\d+", join_posts)
        type_ = list(map(lambda x: "post" if x=="wall" else x, type_))  # api.likes.delete не принимает тип "wall"
        owner_id = re.findall(r"/[A-Za-z]+(-?\d+)_\d+", join_posts)
        post_id = re.findall(r"/[A-Za-z]+-?\d+_(\d+)", join_posts)
        return zip(type_, owner_id, post_id, posts)

    def log(self, message: str, d_type: str,  link: str, err: Exception = None):
        print(f"[{OBJECTS_TYPES[d_type]}] {self.counters[d_type][0]}/{self.counters[d_type][1]} "
              f"Всего: {self.counters['total'][0]}/{self.counters['total'][1]}: {message} {link}")
        if err is not None:
            print(err)

    async def delete_object(self, api_method, api_args, log_args):
        try:
            await api_method(**api_args)
            self.counters[log_args[0]][0] += 1
            response = "Успешно удалено"
        except VkAPIError as err:
            response = VkErrorInfo.error.get(err.error_code, f"VkAPI Неизвестная ошибка.\n{err}")
        except Exception as err:
            log_args[-1] = err
            response = f"Неизвестная ошибка. {type(err)}"
        self.counters["total"][0] += 1
        self.log(response, *log_args)

    async def delete_comment(self, owner_id, item_id, link):
        api_args = {"owner_id": owner_id, "item_id": item_id}
        log_args = [COMMENTS, link, None]
        await self.delete_object(self.api.wall.deleteComment, api_args, log_args)

    async def delete_like(self, type_, owner_id, item_id, link):
        api_args = {"type": type_, "owner_id": owner_id, "item_id": item_id}
        log_args = [LIKES, link, None]
        await self.delete_object(self.api.likes.delete, api_args, log_args)

    async def run(self):
        async with aiovk.TokenSession(access_token=self.access_token) as ses:
            self.api = aiovk.API(ses)
            credentials = {}
            for delete_items, path_ in self.paths.items():
                credentials[delete_items] = self._caller[delete_items][0]()  # вызов get_credentials по ключу delete_items
                self.counters["total"][1] += self.counters[delete_items][1]

            tasks = []

            for delete_items, creds in credentials.items():
                for cr in creds:
                    if len(tasks) == self.max_tasks:
                        await asyncio.gather(*tasks)
                        del tasks[:]
                        if self.random_timeout:
                            await asyncio.sleep(random.uniform(*self.random_timeout))
                        else:
                            await asyncio.sleep(self.timeout)
                    # вызов delete_comment/like по ключу delete_items
                    tasks.append(asyncio.create_task(self._caller[delete_items][1](*cr)))
            await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Удаление всех комментариев/лайков из вк")
    parser.add_argument(
        f"--{TOKEN}",
        help="Access token для vk api (как получить смотреть README.MD)",
        type=str,
        required=True
    )
    parser.add_argument(
        f"--{COMMENTS}",
        help="Путь к выгруженным комментариям",
        type=str
    )
    parser.add_argument(
        f"--{LIKES}",
        help="Путь к выгруженным лайкам",
        type=str
    )
    parser.add_argument(
        f"--{TIMEOUT}",
        help="Время между запросами. По умолчанию 1.0 сек.",
        type=float,
        default=1
    )
    _rand_timeout = parser.add_argument(
        f"--{RANDOM_TIMEOUT}",
        help=f"Случайное время между запросами.(Защита от капчи) По умолчанию отключено. Пример: --{RANDOM_TIMEOUT} 2 4",
        type=float,
        nargs=2
    )
    parser.add_argument(
        f"--{MAX_TASKS}",
        help="Максимальное число async задач (Для защиты от капчи). По умолчанию 1",
        type=int,
        default=1
    )
    args = parser.parse_args()

    paths = {}
    path_args = (COMMENTS, LIKES)

    for arg_n, val_n in vars(args).items():
        if arg_n in path_args and val_n is not None:
            paths[arg_n] = val_n
    try:

        assert paths, f"Не введён ни один путь(к комментариям или лайкам. --{COMMENTS}, --{LIKES})\n{parser.format_usage()}"

        if isinstance(args.random_timeout, list):
            if args.random_timeout[0] > args.random_timeout[1] or args.random_timeout[0] < 0:
                raise argparse.ArgumentError(_rand_timeout, f"{RANDOM_TIMEOUT} аргументы должны быть >= 0 и a <= b ")
        processor = VkDeleter(paths=paths, access_token=args.token,
                                      timeout=args.timeout, max_tasks=args.max_tasks, random_timeout=args.random_timeout)
        asyncio.run(processor.run())
    except KeyboardInterrupt:
        pass
    except Exception as err:
        print(err)
    os.system('pause')
