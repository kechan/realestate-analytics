"""Dependencies belonging to the standalone serving API."""
from fastapi import Request


def get_repository(request: Request):
    return request.app.state.repository
