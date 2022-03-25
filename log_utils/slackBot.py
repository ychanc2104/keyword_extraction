import requests
import argparse
import os, json
from definitions import ROOT_DIR

'''
傳送警告信到slack群組中,要把alpaca加入群組才能運作
Date: 2020-06-29
Author: Ethan
@token: alpaca的token
@channel: 對應slack頻道名稱  範例:G0164UA3B6Y( 以'來客系統警告信'為例slack頻道代碼可以對該頻道名稱右鍵->copy link->https://avividworkspace.slack.com/archives/G0164UA3B6Y)
@message: 訊息  範例:早安
@message: tag user訊息  範例: '<@user_id>早安' (user_id可以從群組Members對想要tag的user右鍵->copy link->https://avividworkspace.slack.com/team/user_id)
@message: tag channel訊息  範例: '<!channel>早安'
'''
class slackBot():
    def __init__(self, channel_name_list, settings_filename='slack_settings.json'):
        self.settings = self._LoadConfig(settings_filename)
        self.token = self.settings["token"]
        self.channel_name_list = channel_name_list
        self.available_channels = self.settings["available_channels"]

    def send_message(self, message):
        for name in self.channel_name_list:
            if name in self.available_channels:
                data = {
                    'token': self.token,
                    'channel': self.available_channels[name],
                    'text': message,
                }
                requests.post('https://slack.com/api/chat.postMessage', data)

    def _LoadConfig(self, settings_filename='slack_settings.json'):
        self.settings_path = os.path.join(ROOT_DIR, 'log_utils', settings_filename)
        with open(self.settings_path) as settings_file:
            settings = json.load(settings_file)
            return settings


if __name__ == "__main__":
    slack = slackBot("clare_test")
    slackBot(["clare_test"]).send_message("HI!!! test")