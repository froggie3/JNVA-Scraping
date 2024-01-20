from typing import Dict, List, TypeAlias
import requests

Response: TypeAlias = requests.models.Response
Thread: TypeAlias = Dict[str, int | str]
Threads: TypeAlias = Dict[str, Thread]
ThreadsChunk: TypeAlias = List[Dict[str, str]]
Post: TypeAlias = Dict[str, int | str]
Posts: TypeAlias = Dict[int, Post]
