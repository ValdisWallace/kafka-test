import logging
from json import dumps

from kafka import KafkaProducer

# Enable logging
logging.basicConfig(
    format='%(asctime)s %(levelname)s [%(name)s] %(message)s', level=logging.INFO
)

log = logging.getLogger(__name__)
producer = KafkaProducer(bootstrap_servers=['m1-pr-priemv-ppmb-test01.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test02.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test03.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test04.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test05.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test06.m1.paym.local:9092',
                                            'm1-pr-priemv-ppmb-test07.m1.paym.local:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         key_serializer=str.encode,
                         acks=-1)


def send(message: dict) -> None:
    key = str(message.get('cycleId'))
    future = producer.send('msm.dwh.test', key=key, value=message)
    result = future.get(timeout=3)
    log.info(f'Sent message key: {key}')


def process() -> None:
    log.info('String process send to kafka topic')

    for i in range(1001, 2001):
        message = {"cycleId": i, "btId": i, "fday": "2022-08-12", "agentId": i, "isDebt": False,
                   "isRko": True, "isBlock": False, "merchantExtid": "3-35X4JUMSK",
                   "merchantName": "ИП Никифоров Арефий Павлович", "merlocExtid": "12_408056",
                   "merlocName": "mKvbMPRAYhLEdgDUcVHFqyYGkFaNGKDqqfotUIqHkcUoGjQqxUALJmNEwvHToYYNsYUGYMWcPISUKBdqGnM",
                   "systemId": "12", "submerchantId": 408004, "mmaShopId": 5382495197592764663,
                   "programType": "ECOMM_DIR_D", "openingDate": "2022-08-01", "closingDate": "2022-08-02",
                   "operCount": 6}
        send(message)

    log.info('End process')


def print_hi(name) -> None:
    log.info(f'Hi, {name}')


if __name__ == '__main__':
    process()
