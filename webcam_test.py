# test the database server with an opencv webcam stream, posting jpeg images to the server at database 'db1', key 'image'
import aiohttp
import cv2
import requests
import time
import numpy as np


image_url = 'http://localhost:8888/db1/image'

print('Testing webcam dictionary http server')
print('Url: ' + image_url)
print('Press Ctrl+C to exit')

async def webcam_server_test_asyncio():
    # get the webcam
    cap = cv2.VideoCapture(0)

    while True:
        # get the image from the webcam
        ret, frame = cap.read()

        # encode the image as a jpeg
        ret, jpeg = cv2.imencode('.jpg', frame)

        # try to post the image to the server
        try:
            async with aiohttp.ClientSession() as session:
                async with session.put('http://localhost:8888/db1/image', data=jpeg.tobytes(), headers={'Content-type': 'image/jpeg'}) as r:
                    #print(r.headers)
                    pass
        except:
            print('Error posting image, trying again')
            pass

        # try to get the image from the server
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get('http://localhost:8888/db1/image') as r:
                    #print(r.headers)
                    # decode the image
                    image = cv2.imdecode(np.frombuffer(await r.read(), np.uint8), cv2.IMREAD_COLOR)
                    # show the image
                    cv2.imshow('image', image)
                    cv2.waitKey(1)
        except:
            print('Error getting image, trying again')
            pass



if __name__ == '__main__':
    #webcam_server_test()
    import asyncio
    loop = asyncio.get_event_loop()
    loop.run_until_complete(webcam_server_test_asyncio())
