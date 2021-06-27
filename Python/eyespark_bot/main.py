import os
import telebot
from dotenv import load_dotenv

#making the .env file accessible to our program as your source of environment variables
load_dotenv()

#This key was created with BotFather: "is the one bot to rule them all"
API_KEY =  os.getenv('API_KEY')
bot = telebot.TeleBot(API_KEY)

@bot.message_handler(commands=['greet'])
def greet(message):
  bot.reply_to(message, "Hey sparky! Hows it going?")

def hello_message(message):
  request = message.text.split()
  if len(request) < 2 or request[0].lower() not in "hello":
    return False
  return True

@bot.message_handler(commands=['hi'])
def hi(message):
  bot.send_message(message.chat.id, "Hi there!")

@bot.message_handler(func=hello_message)
def hello(message):
  request = message.text.split()
  bot.send_message(message.chat.id, "Hello there! You just wrote me " + str(len(request)) + " words")

#keep checking for messages
bot.polling()