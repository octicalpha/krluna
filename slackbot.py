import os
import time
import re
from slackclient import SlackClient
import subprocess

from util import read_conf, run_cmd

conf = read_conf("./config.json")
# instantiate Slack client
slack_client = SlackClient(conf['slack']['bot']['token'])
# starterbot's user ID in Slack: value is assigned after the bot starts up
starterbot_id = None

# constants
RTM_READ_DELAY = 1  # 1 second delay between reading from RTM
EXAMPLE_COMMAND = "do"
MENTION_REGEX = "^<@(|[WU].+)>(.*)"


def parse_bot_commands(slack_events):
    for event in slack_events:
        if event["type"] == "message" and not "subtype" in event:
            user_id, message = parse_direct_mention(event["text"])
            if user_id == starterbot_id:
                return message, event["channel"]
    return None, None


def parse_direct_mention(message_text):
    matches = re.search(MENTION_REGEX, message_text)
    # the first group contains the username, the second group contains the remaining message
    return (matches.group(1), matches.group(2).strip()) if matches else (None, None)


def handle_command(command, channel):
    """
        Executes bot command if the command is known
    """
    # Default response is help text for the user
    default_response = "Not sure what you mean. Try *{}*.".format(EXAMPLE_COMMAND)

    # Finds and executes the given command, filling in response
    response = None
    # This is where you start to implement more commands!
    if command.startswith(EXAMPLE_COMMAND):
        response = "Sure...write some more code then I can do that!"

    cmd = command.split()
    args = []
    if len(cmd) != 0:
        command = cmd[0]
        args = cmd[1:]

    os.chdir(os.path.abspath(os.path.dirname(__file__)))
    if command == 'restart':
        response = run_cmd("supervisorctl restart chopper_spider")
    if command == 'stop':
        response = run_cmd("supervisorctl stop chopper_spider")
    if command in ('benefit', 'status'):
        response = run_cmd("python tools.py benefit")
    if command in ('unfinish', 'unfinished'):
        response = run_cmd("python tools.py unfinish")
    if command in ('apm', 'add_price_monitor'):
        response = run_cmd("python tools.py add_price_monitor --low %s --high %s" % (args[0], args[1]))
    if command in ('rpm', 'remove_price_monitor'):
        response = run_cmd("python tools.py remove_price_monitor")
    if command in ('spm', 'show_price_monitor'):
        response = run_cmd("python tools.py show_price_monitor")

    # Sends the response back to the channel
    slack_client.api_call(
        "chat.postMessage",
        channel=channel,
        text=response or default_response
    )


if __name__ == "__main__":
    if slack_client.rtm_connect(with_team_state=False):
        print("Starter Bot connected and running!")
        # Read bot's user ID by calling Web API method `auth.test`
        starterbot_id = slack_client.api_call("auth.test")["user_id"]
        while True:
            command, channel = parse_bot_commands(slack_client.rtm_read())
            if command:
                handle_command(command, channel)
            time.sleep(RTM_READ_DELAY)
    else:
        print("Connection failed. Exception traceback printed above.")
