class LambdaError(Exception):
    """Class to lambda error handler."""

    def __init__(self, *args):
        self.message = args[0] if args else None

    def __str__(self):
        """Message detail error."""
        return f"[Lambda Error] {self.message or 'No details.'}"
