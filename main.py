from os import environ as env

import flask
import requests
from bs4 import BeautifulSoup

import helpers

app = flask.Flask(__name__)


def update_all_users() -> tuple:

    messages = []

    users = helpers.get_setting('users')

    if not users or ',' not in users:
        return (False, 'No users found in settings')
    
    users = users.split(',')

    for user in users:
        messages.append((user, update_user(user)))

    return messages


def update_user(user: str, mute: bool = False) -> list:

    print(f"Scanning user: {user}")

    found = {}
    reviews = {}
    messages = []
    webhooks = []

    discord_webhook = helpers.get_setting('discord_webhook')

    if discord_webhook != 'NF':
        webhooks.append(discord_webhook)

    existing = helpers.get_existing_games(user)

    offset = 0
    data = None

    while offset == 0 or (data and data.text != '\n' and data.text != '\n\n'):

        url = f"https://stash.games/users/{user}/reviews/items?offset={offset}&limit=100"
        data = BeautifulSoup(requests.get(url).text, 'html.parser')

        for game in data.find_all('div', {'class': 'recent-review'}):
            game_id = game.get('data-link').split('/')[2]
            rating = game.find('span', {'class': 'game__review-rating'}).get_text()
            reviews[game_id] = rating

        offset += 100

    for tag in ['beaten', 'archived', 'playing', 'want']:

        offset = 0
        data = None

        while offset == 0 or (data and data.text != '\n' and data.text != '\n\n'):

            url = f"https://stash.games/users/{user}/statuses?include=tags:{tag}&offset={offset}&limit=100"
            data = BeautifulSoup(requests.get(url).text, 'html.parser')

            for game in data.find_all('a', {'class': 'games-list__item-link'}):

                game_id = game.get('href').split('/')[2]
                name = game.get('data-text')
                review = reviews.get(game_id, None)

                game_data = {
                    'user': user,
                    'game_id': game_id,
                    'game_name': name,
                    'status': tag,
                    'rating': None if not review else float(review)
                }

                found[game_id] = game_data

            offset += 100

    added = []

    if not found:
        return

    for game in found.values():
        if game != existing.get(game.get('game_id')):

            added.append(game)

            match game.get('status'):

                case 'want':
                    phrase = 'wants to play'
                case 'playing':
                    phrase = 'has started playing'
                case 'beaten':
                    phrase = 'has finished'
                case 'archived':
                    phrase = 'has stopped playing'
                case _:
                    continue

            gamelink = f"[{game.get('game_name')}](https://stash.games/games/{game.get('game_id')}/)"
            message = f"* **{user}** {phrase} _{gamelink}_"

            if game.get('rating'):
                message += f" **(Rating: {int(game.get('rating'))}/10)**"

            messages.append(message)

    if added:
        helpers.update_existing(user, added)
        if not mute and webhooks:
            for webhook in webhooks:
                helpers.send_to_webhook(messages, webhook)

    return added


@app.route('/add/<user>')
def add_user(user):

    users = helpers.get_setting('users')

    if not users or ',' not in users:
        userlist = []
    else:
        userlist = users.split(',')

    if user in userlist:
        return f"{user} already added"
    
    userlist.append(user)

    helpers.update_setting('users', ','.join(userlist))
    update_user(user, mute=True)

    return f"Added {user}"


@app.route('/')
def index():
    added = update_all_users()
    return added


if __name__ == '__main__':
    app.run()
