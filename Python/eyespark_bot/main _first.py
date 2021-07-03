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

@bot.message_handler(commands=["start"])
def inline(message):
    key = telebot.types.InlineKeyboardMarkup()
    but_1 = telebot.types.InlineKeyboardButton(text="NumberOne", callback_data="NumberOne")
    but_2 = telebot.types.InlineKeyboardButton(text="NumberTwo", callback_data="NumberTwo")
    but_3 = telebot.types.InlineKeyboardButton(text="NumberTree", callback_data="NumberTree")
    but_4 = telebot.types.InlineKeyboardButton(text="Number4", callback_data="Number4")
    key.add(but_1, but_2, but_3, but_4)
    bot.send_message(message.chat.id, "ВЫБЕРИТЕ КНОПКУ", reply_markup=key)

@bot.callback_query_handler(func=lambda c:True)
def inlin(c):
    if c.data == 'NumberOne':
        bot.send_message(c.message.chat.id, 'Button 1')
    if c.data == 'NumberTwo':
        bot.send_message(c.message.chat.id, 'Button 2')
    if c.data == 'NumberTree':
        bot.send_message(c.message.chat.id, 'Button 3')
    if c.data == 'Number4':
        bot.send_message(c.message.chat.id, 'Button 4')


#keep checking for messages
bot.polling()