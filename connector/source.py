# placeholder connector class
class FitSenseConnector:
    def schema(self):
        return {"streams": ["activities", "sleep", "hr_series"]}
    def read(self, stream, state):
        yield {"example": True}
