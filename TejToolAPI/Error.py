class TejToolAPIError(Exception):
    def __init__(self, message) -> None:
        # super().__init__()
        self.message = message
    def __str__(self) -> str:
        return self.message