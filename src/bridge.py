from enum import Enum
from src.core import download_cpe, send_cpe, confirm_cpe

class MSG_TYPE(Enum):
    ERROR = 0
    SUCCESS = 1

class Message:
    def __init__(self):
        self.type = None
        self.content = ""

    def set_type(self, type):
        self.type = type

    def set_content(self, content):
        self.content = content


class Bridge:
    def __init__(self):
        pass

    def call_download_cpe(self):
        msg = Message()
        try:
            result = download_cpe()
            msg.set_type(MSG_TYPE.SUCCESS)
            msg.set_content(result)
        except Exception as e:
            msg.set_type(MSG_TYPE.ERROR)
            msg.set_content(e.args)
        return msg

    def call_send_cpe(self):
        msg = Message()
        try:
            result = send_cpe()
            msg.set_type(MSG_TYPE.SUCCESS)
            msg.set_content(result)
        except Exception as e:
            msg.set_type(MSG_TYPE.ERROR)
            msg.set_content(e.args)
        return msg

    def call_confirm_cpe(self):
        msg = Message()
        try:
            result = confirm_cpe()
            msg.set_type(MSG_TYPE.SUCCESS)
            msg.set_content(result)
        except Exception as e:
            msg.set_type(MSG_TYPE.ERROR)
            msg.set_content(e.args)
        return msg
