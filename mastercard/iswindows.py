from sys import platform
class IsWindows():
    def __init__(self):
        self.is_windows = self.is_windows()

    def is_windows(self):
        if (platform =="win32" or platform == "win64"):
            return True
        elif (platform == "linux" or platform == "linux2"):
            return False
        else:
            raise Exception(f"{platform} is not windows or linux?")
