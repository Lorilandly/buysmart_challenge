import zlib
import time
# time.time()
import json
import base64
import asyncio
from pymongo import MongoClient
from websockets.sync.client import connect

MONGODB_URL = 'mongodb://root:example@localhost:27017/'

def compress_gzip(msg):
    compressed = zlib.compress(msg.encode('utf-8'))
    return compressed


def convert_buylink(asin: str) -> str:
    return f'https://keepa.com/#!product/1-{asin}'


def convert_image_url(dec_list):
    link = 'https://m.media-amazon.com/images/I/'
    for n in dec_list:
        link = link + chr(n)
    link = link.replace('.jpg', '._SS180_.jpg')
    return link


def main():
    headers = {
        'Connection': 'Upgrade',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36',
        'Upgrade': 'websocket',
        'Origin': 'https://keepa.com',
        'Sec-WebSocket-Version': '13',
        'Accept-Encoding': 'gzip, deflate, br, zstd',
        'Accept-Language': 'en-US,en;q=0.9,ja;q=0.8'
    }
        
    payload1 = 'eJxFkd9P2zAQx/8Vy08gRY3txHHSaQ8BqQNCCywz63hBjrkkXpMmTUxbivjf50xI2A/2fe7HV3f3jntlazzHryMM/gjWmm01Yg/btx4crsDmX7BVx7RyOCDT8XDjIjYAvZrprnX+V2f706vcZ9mdTNMon88IOlsqbba2G+tv6HproUEOoLscrRElz5Q/i3OU9n0Dv6HIjPV5IGZBhM6yq1/LWw81ZgPoB+hNd44u66FrwacsmJHpolyVajCfKU586uaN8NWt/CvzWNxfPKTfHTYveE4Ji7iHyx7P37FxcXRz3J1i5dyDs7T/85QEi0dylY7Zsr6hj4vDxf0qlmTM9TG/lPtVLjubm+ggDzfX2b77k6Vyze+qXQ1aLna6fVJPbZ899PVOrhdt2fC2+q9unbigYZQkjCaBYB8e3sMwmm7rdBlhIQmZmCbn1uBIyYjiSQkJkDBinAvxwiPOmSaCF1CGpabclYIkVkWhQVMVFoqzMg4Tpbjm+OMfDu+KnA=='
    payload2 = 'eJytVE1vGjEQ/S973rReWAjJLUoTNVLSRCHiwuYw2R1YK16b2l5Ig/jvHdsLGBr1Q6qEjHhvGHvevJl1sgBbJ+dJa1B/Nmgtl3OTpIn9sUCCCRnvQaFKEGOrNMwduS6SCkGYp7ptXsb8HYvkvEhYkaRFIkDOWwrzUOYhbICLCWo+41hFoVyWoq3wDi1EKL5ZjQ1ec2FRR7i/8RG/t2jsJwHGem5dFAWVQvfR9zlL3Vel6EJ5U3nIHZk7PIVv/spLsDhXmqPxMdPpc/rXn2efqHv7R4kyNhj0z0b/lm+heYlPpH2XiAW4QmHhkSQN9U1Z2svy03zUH+anUcQD6hKljQLzXyMNCDQU8hqFnWTpSRbostX6MMeAsfTsjHVPabh8BGcIT1J+L4O5VStqiMdm1CA8gO9nM+rhMXffEkFuKl+PKMutwDGCLutd73at4+7tc7yS8CLIRo63ut3mDHaJyX3aWeC0srw8/J+hcgROQHOqTMlDsgay25LjKrQkruBB8wavnJcMXwa19nTTGvsVlnjRwLuSH0ngIr6pPwQZpa0zhIf7odfktn1/3MF28qxAY61omi+VrLirpnNSL+2neTroPNDNtKf87LgC6ZowPpvgRdQP24nKBmzTjd8MWuGeTWNcerluJMm6BOEHsTcahTmda1jUY9oX8Vbww3sY320LYyfc8DDMUewdOboGGfCwRnZjfQxFgL/88J5sGNLu7H8jl6hN/DooS9XKoPYebUCSChPSJwJdV74EKbbohhbk9m20HDO3L7sl2P38jXYU4ZSjIKkuVbugtl0rfSFQW7N1A1E7wx0xJZCFb912RgKn3ruB+q/nc5pwqi1jveEwTZx8VIJ7OuvlLO+dJpuf+2sdvA=='
    payload3 = 'eJx1Uk1P4zAQ/S9zNlonTSjNbYVY7UqrpSorLoiDcabtCMcuYycFKv4749LwKW72ezOeN+95BxuT1tBARB7I4o8WjYugID1sUOBtPmdsgXc9xgTNDmzwMXFv5eJ755Q8sZJaLYWhM+T/tNJYSB/eW9e3eGoSrgITRmiurhWQ/woXuq4nsxNhNywy/sv0DGsBWnTJLIzPM660KotqWp1MjqvpyM2RLfo0llSfaqJxGIW8HQuOCnVUCGF75nd9tdZqNtN5ZEd+YRL5FTTyGlD8G7b75ZdiDr4B58sl8jv0vBfoIgV7+womSg4v0LDNLosplLWs8MybG4filDi57/1FLiG/wof25QvKIZEda6MIc3hpmERj8CO8NnGBA6FE9qZoztThWc4h0iBbHoiuj+m3GfBnZx6D/7hG5v6Fb+kYOOV4oJmI/5LhwT/QstzWMK5DH/E0+JayupxiqSaqUnXOAlO2VcAdZKnykHycJ0kdeb7/RUWdryQOFLo8nioYkON+Syh1WemqnMLTM/uk56Q='

    # create ws connection
    formatted_products = []
    with connect("wss://dyn-3.keepa.com/apps/cloud/?app=keepaWebsite&version=2.0", additional_headers=headers) as ws:
        # get ws response
        rcv = ws.recv()
        print('Init response recv')

        # send ws request1
        raw = base64.b64decode(payload1)
        ws.send(raw)
        print('1st msg sent')
        # base64_msg = base64.b64encode(encoded)
        # print(base64_msg.decode('utf-8'))
        rcv = ws.recv()
        print('1st response recv')

        # send ws request2
        raw = base64.b64decode(payload2)
        ws.send(raw)
        print('2nd msg sent')

        # send ws request3
        raw = base64.b64decode(payload3)
        ws.send(raw)
        print('3rd msg sent')

        time.sleep(1)
        # get ws response
        rcv = ws.recv()
        print('2nd response recv')
        rcv = ws.recv()
        print("Final products retrieved")
        deflate = zlib.decompress(rcv, -zlib.MAX_WBITS)
        json_str = deflate.decode('utf-8')

        parsed_dict = json.loads(json_str)
        products = parsed_dict['deals']['dr']

        counter = 0
        try:
            for product in products:
                p = {
                    'No.': counter,
                    'title' : product['title'],
                    'buylink' : convert_buylink(product['asin']),
                    'price' : float(product['current'][0]) / 100,
                    'old_price' : float(product['avg'][0][0]) / 100,
                    'image_url' : convert_image_url(product['image']),
                }
                formatted_products.append(p)

                counter += 1
        except Exception as e:
            print("Data corruption, please try again!")
            exit(1)


    db = DB()
    db.save_lulu_prods(formatted_products)
    print("Done")
        
class DB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URL)
        self.db = self.client['crawler']
        self.col = self.db['keepa']

    def save_lulu_prods(self, q):
        self.col.insert_many(q)


if __name__ == '__main__':
    main()
