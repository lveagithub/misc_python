import requests
import pathlib
from PIL import Image
from base64 import decodebytes, encodebytes
from datetime import timedelta, datetime


curr_dir_path = pathlib.Path(__file__).parent.absolute()
image_dir = str(curr_dir_path) + "/test_images/"
#test_photos = {'file': open(image_dir + "0_left.jpg" ,'rb')} #No Face
#test_photos = {'file': open(image_dir + "Selfie.jpg" ,'rb')} #Multiple Faces
test_photos = {'file': open(image_dir + "test1.jpg" ,'rb')} #Single Face

resp = requests.post(url='http://tov-m-LoadB-RUXY2HD2AGFL-242d67356c99bf24.elb.us-east-1.amazonaws.com:80/eyesDiagnosis', files=test_photos)

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

if resp.status_code != 200:
    # This means something went wrong and we want to know what happened
    raise Exception('POST /eyesDiagnosis/ {}'.format(resp.status_code))
json_pairs = resp.json().items()
for key, value in json_pairs:
    print('{} {}'.format(key, value))
    #We want to get the images
    if key == 'data':
        dic_data = value[0]
        for data_key in dic_data:
            if data_key == 'left_eye_im':
                write_reponse_image(dic_data[data_key])
            if data_key == 'right_eye_im':
                write_reponse_image(dic_data[data_key])