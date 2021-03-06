import os
import telebot
from dotenv import load_dotenv

import requests
import pathlib
import time
from PIL import Image
from base64 import decodebytes, encodebytes
from datetime import timedelta, datetime
import cv2

from telebot import types

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import MaxRetryError

knownUsers = []  # todo: save these in a file,
userStep = {}  # so they won't reset every time the bot restarts

#cap = cv2.VideoCapture(1)
#cap.set(3,480)
#cap.set(4,360)

CURR_DIR_PATH = pathlib.Path(__file__).parent.absolute()
API_URL = 'http://tov-m-LoadB-9DHGIWGB3RBL-fa6e97b58b0ec9fa.elb.us-east-1.amazonaws.com:80/eyesDiagnosis'

commands = {  # command description used in the "help" command
              'start': 'Get used to the bot',
              'help': 'Gives you information about the available commands',
              #'sendLongText': 'A test using the \'send_chat_action\' command',
              'getImage': 'A test using multi-stage messages, custom keyboard, and media sending'
}

imageSelect = types.ReplyKeyboardMarkup(one_time_keyboard=True)  # create the image selection keyboard
#imageSelect.add('camera1', 'camera2')

hideBoard = types.ReplyKeyboardRemove()  # if sent as reply_markup, will hide the keyboard


# error handling if user isn't known yet
# (obsolete once known users are saved to file, because all users
#   had to use the /start command and are therefore known to the bot)
def get_user_step(uid):
    if uid in userStep:
        return userStep[uid]
    else:
        knownUsers.append(uid)
        userStep[uid] = 0
        print ("New user detected, who hasn't used \"/start\" yet")
        return 0


# only used for console output now
def listener(messages):
    """
    When new messages arrive TeleBot will call this function.
    """
    for m in messages:
        if m.content_type == 'text':
            # print the sent message to the console
            print (str(m.chat.first_name) + " [" + str(m.chat.id) + "]: " + m.text)

#making the .env file accessible to our program as your source of environment variables
load_dotenv()

#This key was created with BotFather: "is the one bot to rule them all"
API_KEY =  os.getenv('API_KEY')
bot = telebot.TeleBot(API_KEY)

bot.set_update_listener(listener)  # register listener


# handle the "/start" command
@bot.message_handler(commands=['start'])
def command_start(m):
    cid = m.chat.id
    if cid not in knownUsers:  # if user hasn't used the "/start" command yet:
        knownUsers.append(cid)  # save user id, so you could brodcast messages to all users of this bot later
        userStep[cid] = 0  # save user id and his current "command level", so he can use the "/getImage" command
        bot.send_message(cid, "Hello, stranger, let me scan you...")
        bot.send_message(cid, f"Scanning complete, I know you now {m.chat.first_name}")
        command_help(m)  # show the new user the help page
    else:
        bot.send_message(cid, "I already know you, no need for me to scan you again!")


# help page
@bot.message_handler(commands=['help'])
def command_help(m):
    cid = m.chat.id
    help_text = "The following commands are available: \n"
    for key in commands:  # generate help text out of the commands dictionary defined at the top
        help_text += "/" + key + ": "
        help_text += commands[key] + "\n"
    bot.send_message(cid, help_text)  # send the generated help page


# chat_action example (not a good one...)
#@bot.message_handler(commands=['sendLongText'])
#def command_long_text(m):
#    cid = m.chat.id
#    bot.send_message(cid, "If you think so...")
#    bot.send_chat_action(cid, 'typing')  # show the bot "typing" (max. 5 secs)
#    time.sleep(3)
#    bot.send_message(cid, ".")

def consume_api(image_path):
    test_photos = {'file': open(image_path ,'rb')}

    t0 = time.time()
    try:
        resp = requests.models.Response()
        resp.status_code = 400

        #Controlling http Session
        session_ = requests.Session()
        allowed_methods_ = frozenset({'HEAD', 'GET', 'TRACE', 'POST'})
        status_forcelist_ = frozenset({502, 503, 504})
        retries_ = Retry(total=5, backoff_factor=1,allowed_methods = allowed_methods_, status_forcelist=status_forcelist_)
        session_.mount('http://', HTTPAdapter(max_retries=retries_))

        # "httpstat.us" It's a Simple service for generating different HTTP codes. 
        # It's useful for testing how your own scripts deal with varying responses.
        #resp_test = session_.get("http://httpstat.us/503")
        #print(f"The temporal response: {resp_test}")
        
        resp = session_.post(url=API_URL, files=test_photos, headers={'User-Agent': 'Mozilla/5.0'})

        #resp = requests.post(url='http://tov-m-LoadB-18I5O5VTXDK6S-899453c1eafe5051.elb.us-east-1.amazonaws.com:80/eyesDiagnosis', files=test_photos, headers={'User-Agent': 'Mozilla/5.0'})
        #resp = requests.post(url='http://10.188.112.39:80/eyesDiagnosis', files=test_photos)

    except MaxRetryError as e:
        print(f"Failed due to: {e.reason}")    
    except Exception as e:
        if hasattr(e, 'message'):
            print(f"Failed due to {e.message}")
        else:
            print(f"Failed due to: {e}")  
        
    finally:
        t1 = time.time()
        proc_time = round(t1-t0)
        print(f"Took, {proc_time}, seconds")
        #print(f"Retries info:{retries_.__dict__}")
        session_.close()

    return resp, proc_time

def write_reponse_image(image_64_encode):
    #Parsed images directory
    curr_dir_path = pathlib.Path(__file__).parent.absolute()
    parsed_img_dir = '/parsed_images/'  
    parsed_img_dir_full = str(curr_dir_path) + str(parsed_img_dir)

    #Decoding image_64_encode
    #image_64_decode = decodestring(image_64_encode)
    image_64_decode = decodebytes(image_64_encode.encode("ascii"))
    image_result_name_tmp = datetime.now().strftime("%Y%m%d%H%M%S%f") + '.jpg'
    tmp_img_name = parsed_img_dir_full + image_result_name_tmp
    image_result = open(tmp_img_name, 'wb') # create a writable image and write the decoding result
    image_result.write(image_64_decode)
    return tmp_img_name

# user can chose an image (multi-stage command example)
@bot.message_handler(commands=['getImage'])
def command_image(m):
    cid = m.chat.id
    bot.send_message(cid, "Please choose your image now", parse_mode='HTML', reply_markup=imageSelect)  # show the keyboard
    userStep[cid] = 1  # set the user to the next step (expecting a reply in the listener now)


# if the user has issued the "/getImage" command, process the answer
@bot.message_handler(content_types=['photo'], func=lambda message: get_user_step(message.chat.id) == 1)
def msg_image_select(m):
    cid = m.chat.id
    text = m.text

    image_dir = str(CURR_DIR_PATH) + "/parsed_images/"

    bot.send_chat_action(cid, 'upload_photo')

    fileID = m.photo[-1].file_id
    file_info = bot.get_file(fileID)
    print ('file.file_path =', file_info.file_path)
    downloaded_file = bot.download_file(file_info.file_path)

    generated_img_name = datetime.now().strftime("%Y%m%d%H%M%S%f") + pathlib.Path(file_info.file_path).suffix

    with open(image_dir + generated_img_name, 'wb') as new_file:
        new_file.write(downloaded_file)

    bot.send_message(chat_id = cid, text = "<b>Processing and predicting ...</b>", parse_mode='HTML')

    resp, proc_time = consume_api(image_path = image_dir + generated_img_name)

    #Initializing variables
    category_ = ""
    left_eye_im_desc_ = ""
    left_eye_im_diagnosis_ = ""
    left_eye_im_ = ""
    right_eye_im_desc_ = ""
    right_eye_im_diagnosis_ = ""
    right_eye_im_ = ""
    tmp_img_name_right = ""
    tmp_img_name_left = ""
    message_ = ""

    if resp.status_code != 200:
        # This means something went wrong and we want to know what happened
        raise Exception('POST /eyesDiagnosis/ {}'.format(resp.status_code))
    json_pairs = resp.json().items()
    for key, value in json_pairs:
        print('{} {}'.format(key, value))
        if key == 'category':
            category_ = value
        if key == "message":
            message_ = value
        #We want to get the images
        if key == 'data':
            if len(value) > 0:
                dic_data = value[0]
                for data_key in dic_data:
                    if data_key == "left_eye_im_desc":
                        left_eye_im_desc_ = dic_data[data_key]
                    if data_key == "left_eye_im_diagnosis":
                        left_eye_im_diagnosis_ = dic_data[data_key]
                    if data_key == "right_eye_im_desc":
                        right_eye_im_desc_ = dic_data[data_key]
                    if data_key == "right_eye_im_diagnosis":
                        right_eye_im_diagnosis_ = dic_data[data_key]
                    if data_key == 'left_eye_im':
                        left_eye_im_ = dic_data[data_key]
                        tmp_img_name_left = write_reponse_image(left_eye_im_)
                    if data_key == 'right_eye_im':
                        right_eye_im_ = dic_data[data_key]
                        tmp_img_name_right = write_reponse_image(right_eye_im_)

    html_string = f"<b>Report: {category_}</b>"
    bot.send_message(cid, html_string, parse_mode='HTML')
    
    if message_ == "success":
        html_string = f"<b>{left_eye_im_desc_}: {left_eye_im_diagnosis_}</b>"
        bot.send_message(cid, html_string, parse_mode='HTML')

        if len(tmp_img_name_left) > 0:
            bot.send_photo(cid, open(tmp_img_name_left, 'rb'), reply_markup=hideBoard)

        #html_string = f"<b>left_eye_im_diagnosis_: {left_eye_im_diagnosis_}</b>"
        #bot.send_message(cid, html_string, parse_mode='HTML')

        #html_string = f"<b>left_eye_im_: {category_}</b>"
        #bot.send_message(cid, html_string, parse_mode='HTML')

        html_string = f"<b>{right_eye_im_desc_}: {right_eye_im_diagnosis_}</b>"
        bot.send_message(cid, html_string, parse_mode='HTML')

        if len(tmp_img_name_right) > 0:
            bot.send_photo(cid, open(tmp_img_name_right, 'rb'), reply_markup=hideBoard)

        #html_string = f"<b>right_eye_im_diagnosis_: {right_eye_im_diagnosis_}</b>"
        #bot.send_message(cid, html_string, parse_mode='HTML')

        #html_string = f"<b>right_eye_im_: {category_}</b>"
        #bot.send_message(cid, html_string, parse_mode='HTML')

        #bot.send_message(chat_id = cid, text = "<b>Processing and predicting ...</b>", parse_mode='HTML')        
    else:
        html_string = f"<b>{message_}</b>"
        bot.send_message(cid, html_string, parse_mode='HTML')

    html_string = f"<b>It took {proc_time} seconds</b>"
    bot.send_message(cid, html_string, parse_mode='HTML')
# filter on a specific message
@bot.message_handler(func=lambda message: message.text == "hi")
def command_text_hi(m):
    bot.send_message(m.chat.id, "I love you too!")


# default handler for every other text
@bot.message_handler(func=lambda message: True, content_types=['text'])
def command_default(m):
    # this is the standard reply to a normal message
    bot.send_message(m.chat.id, "I don't understand \"" + m.text + "\"\nMaybe try the help page at /help")

bot.polling()
