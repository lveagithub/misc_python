import requests
import pathlib
import time
from PIL import Image
from base64 import decodebytes, encodebytes
from datetime import timedelta, datetime

from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from requests.packages.urllib3.exceptions import MaxRetryError


curr_dir_path = pathlib.Path(__file__).parent.absolute()
image_dir = str(curr_dir_path) + "/test_images/"
#test_photos = {'file': open(image_dir + "0_left.jpg" ,'rb')} #No Face
#test_photos = {'file': open(image_dir + "Selfie.jpg" ,'rb')} #Multiple Faces
test_photos = {'file': open(image_dir + "Cataract_eyes_face_2.jpg" ,'rb')} #Single Face

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
    
    resp = session_.post(url='http://tov-m-LoadB-RXZXM237C20Y-611ee7192043f9fb.elb.us-east-1.amazonaws.com:80/eyesDiagnosis', files=test_photos, headers={'User-Agent': 'Mozilla/5.0'})

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
    print(f"Took, {t1-t0}, seconds")
    #print(f"Retries info:{retries_.__dict__}")
    session_.close()

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
        if len(value) > 0:
            dic_data = value[0]
            for data_key in dic_data:
                if data_key == 'left_eye_im':
                    write_reponse_image(dic_data[data_key])
                if data_key == 'right_eye_im':
                    write_reponse_image(dic_data[data_key])