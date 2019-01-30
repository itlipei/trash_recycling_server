import json
import re
import os

from twisted.internet.protocol import Factory, connectionDone
from twisted.internet import reactor, protocol
import logging
import redis

import binascii
import threading

coon0 = redis.Redis(host='192.168.0.117', port=6379, db=13)
coon1 = redis.Redis(host='192.168.0.117', port=6379, db=14, )
init_threading = True
init_state = True


def log(xieyi, info):
    logger = logging.getLogger(xieyi)
    logger.setLevel(logging.INFO)
    fh = logging.FileHandler('data.log')
    fh.setLevel(logging.INFO)
    ch = logging.StreamHandler()
    ch.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    ch.setFormatter(formatter)
    logger.addHandler(fh)
    logger.addHandler(ch)
    logger.info(info)
    logger.removeHandler(fh)
    logger.removeHandler(ch)


def num_xor(num):
    i = 0
    xor = int()
    num_list = []
    for x in range(int(len(num) / 2)):
        num_list.append(str(num[i:i + 2]))
        i = i + 2

    for i in range(len(num_list)):
        x = int(num_list[i], 16)

        i = i + 2
        if i < 0:
            xor = x
        elif i > 0:
            xor = xor ^ x
    return xor


def change_xor(xor):
    xor = hex(num_xor(xor))
    xor = xor[2:]
    if len(xor) < 2:
        xor = "0" + xor
    return str(xor)


class QuoteProtocol(protocol.Protocol):
    def __init__(self, factory):
        self.factory = factory
        self.w = self.factory.w_dict
        self.route = {
            '5016': Route().ox5016,
            '5010': Route().ox5010,
            '5011': Route().ox5011,
            '5012': Route().ox5012,
            '5013': Route().ox5013,
            '5014': Route().ox5014,
            '5017': Route().ox5017,
            '5018': Route().ox5018,
            '5019': Route().ox5019,
            '5810': Route().ox5810,
            '5811': Route().ox5811,
            "5813": Route().ox5813,
            "5814": Route().ox5814,
            "501b": Route().ox501b,
            "501c": Route().ox501c,
            "581c": Route().ox581c,
            "501d": Route().ox501d
        }

    def received_django(self):

        while True:
            keys = coon0.keys()
            keys2 = coon1.keys()
            if len(keys2) > 0:
                for key in keys2:
                    if re.match(r"^\d{16}_[0-9,abc]{4}$", key.decode()):
                        if coon1.hget(key, 'code') == b"1":
                            coon1.delete(key)

            if len(keys) > 0:
                for key in keys:
                    if re.match(r"^\d{16}_[0-9,abc]{4}$", key.decode()):
                        if coon0.hget(key, 'code') == b"0":
                            item = {}
                            # item["state"] = coon0.hget(key, "state")
                            item['protocol'] = coon0.hget(key, 'protocol')
                            item["imei"] = coon0.hget(key, "imei")
                            item['code'] = coon0.hget(key, 'code')
                            item["data"] = coon0.hget(key, 'data')
                            log(item["imei"].decode() + "_" + item["protocol"].decode(), item["data"])
                            coon0.hset(item["imei"].decode() + "_" + item["protocol"].decode(), "code", "1")
                            token = coon0.hget(key, 'token')
                            if item["protocol"] == b"501c":
                                self.w["worker" + item["imei"].decode()] = token

                            elif token:
                                self.w["token" + item['imei'].decode()] = token

                            # 判断是否已经完成置位
                            if coon0.hget(key, 'code') == b"1":
                                self.data_received(item)

                #
                #
                #             pass
                #
                #         else:
                #             pass

    def data_received(self, item):
        """
        接受django的数据
        :param item:
        """
        try:
            send = self.route[item['protocol'].decode()](item)
            self.w[item['imei'].decode()]['tr'].write(send)
            log("发送给终端时的信息", self.w[item['imei'].decode()]['tr'])
        except Exception as e:
            coon0.delete(item['imei'].decode() + "_" + "item")

    def monitor(self):
        """
        监听过期时间，对未应答进行处理
        监听过期时间，对心跳信息未发送处理
        :return:
        """
        r = redis.StrictRedis()
        pub = r.pubsub()
        pub.subscribe('__keyevent@14__:expired')

        for msg in pub.listen():

            info = ((msg['data']))
            if type(info) != int:
                info = info.decode()
                if re.match(r'^heartbeat', info):
                    # 心跳信息过时

                    coon1.hmset(info[9:] + "_5019", {"status": "01", "imei": info[9:], "protocol": "5019", "code": '0'})
                    log(info, '心跳信息未发送，设备已经断开链接')
                elif re.match(r"^reply", info):
                    # 终端未回复

                    tcp_class = self.w[info[5:21]]
                    protocol = info[21:25]
                    try:
                        tcp_class["count"] += 1
                    except Exception as ex:
                        tcp_class["count"] = 1
                    try:
                        tcp_class['tr'].write(tcp_class['send'])
                    except Exception as ex:
                        log(info, '已经回复')
                        tcp_class['count'] = 5
                    if tcp_class["count"] < 3:
                        coon1.setex(info, 3, "time")
                        log(info, "设备未回复")
                    # 发送完三次判断有没有回复
                    if tcp_class["count"] > 2 and tcp_class["count"] < 5:
                        state = json.dumps({"state": "01"})
                        coon1.hmset(tcp_class['id'] + "_" + protocol,
                                    {"imei": tcp_class['id'], "protocol": protocol, "data": state, "code": '0'})
                        tcp_class["count"] = 0
                        log(tcp_class['id'] + protocol, '设备三次未回复应答已经上传错误')

    def django_monitor(self):
        r = redis.StrictRedis()
        pub = r.pubsub()
        pub.subscribe('__keyevent@13__:expired')

        for msg in pub.listen():
            info = (msg['data'])
            if type(info) != int:
                info = info.decode()
                _date_set = {}
                _date_set['state'] = '00'
                id = info[21:23]
                _date_set['id'] = id
                imei = info[5:21]
                token = info[23:]
                coon0.hmset(imei + "_5011",
                            {'protocol': '5011', 'imei': imei, "data": json.dumps(_date_set), "token": token,
                             'code': "0"})

    # 建立连接后的回调函数
    def connectionMade(self):
        #
        log('建立链接', '连接成功')

    def subpackage(self, data):
        """
        接受终端数据对数据中的粘包进行处理
        :param data:
        :return: 返回处理完的正常数据
        """
        _data = binascii.b2a_hex(data).decode()

        _len = int(_data[8:12], 16)
        datalen = int(len(_data) / 2 - 8)

        if _len != datalen:
            # 粘包包处理
            datalist = re.findall(r".*?0d", _data)
            right = []
            test = ""
            for info in datalist:
                _len = int(info[8:12], 16)
                datalen = len(info) / 2 - 8
                a = []
                if _len != datalen:
                    test += info

                else:
                    a.append(info)
                right.extend(a)
            right.append(test)
            for i in right:
                global init_state
                init_state = False
                if i != "":
                    self.dataReceived(i)

                init_state = True

        else:
            # 正确的数据

            return _data

    def dataReceived(self, data):
        """
        接受终端的数据并进行逻辑处理
        :param data:
        :return:
        """
        if init_state:
            data = self.subpackage(data)

        try:
            # data = binascii.b2a_hex(data).decode()
            id = data[12:28]
            log('完整数据--------', data)
            self.w[id] = {}
            self.w[id]["xor"] = (int(data[-4:-2], 16))
            self.w[id]['ip'] = self.transport.getPeer().host
            self.w[id]['title'] = data[0:4]
            self.w[id]['protocol'] = data[4:8]
            self.w[id]['len'] = (data[8:12])
            self.w[id]['id'] = data[12:28]

            self.w[id]['end'] = data[-2:]
            num = len(data) - 4
            self.w[id]['data'] = data[28:num]
            self.w[id]['tr'] = self.transport
            self.w[id]["nums"] = data
            nums = data[4:-4]
            xor = num_xor(nums)

        except Exception as e:
            log('接受终端数据', e)
            # 切割的时候出错说明data中有5d  切割数据不符合转换
            pass
            # 将数据保存等待下次链接时加入
        else:
            if xor == self.w[id]['xor']:
                send = self.route[self.w[id]['protocol']](self.w[id])

                if send != None:
                    self.w[id]['tr'].write(send)

            else:

                pass

    def connectionLost(self, reason=connectionDone):
        """
        断开连接后的回调函数
        :param reason:
        :return:
        """
        # reactor.run()
        log(str(self.transport), '已经断开链接')

    def getQuote(self):
        return self.fount.send.encode()

    def updateQuote(self, quote):
        # quote = quote.decode()

        self.factory.quote = quote


class QuoteFactory(Factory):
    w_dict = {}

    # 数据接收后放在在quote中
    def __init__(self, quote=None):
        global init_threading
        self.quote = quote or str('数据存储').encode("utf-8")
        if init_threading:
            self.q = QuoteProtocol(self)
            t = threading.Thread(target=self.q.received_django)
            t.start()

            t1 = threading.Thread(target=self.q.monitor)
            t1.start()

            t2 = threading.Thread(target=self.q.django_monitor)
            t2.start()
        init_threading = False

    def buildProtocol(self, addr):
        return QuoteProtocol(self)


class Route(object):
    """
    逻辑处理                                                                处理方法
    """
    redis_dict = {}
    w_dict = {}

    def ox5016(self, data):
        """
        接受终端的机器信息
        :param data:
        :return: 成功失败信息
        """

        x1 = data['data'][0:16]
        x2 = data['data'][16:18]
        x3 = data['data'][18:20]
        x4 = data['data'][20:22]
        x5 = data['data'][22:24]
        x6 = data['data'][24:26]
        x7 = data['data'][26:28]
        name = data['id'] + "_5016"
        mess = json.dumps({"x1": x1, "x2": x2, "x3": x3, "x4": x4, "x5": x5, "x6": x6, "x7": x7})
        coon1.hmset(name, {"mess": data["nums"], "imei": data["id"], "protocol": "5016", 'code': '0',
                           "data": mess})
        nums = "58160009" + data['id'] + '00'
        log(data["id"] + '5016', '数据接受成功')
        xor = change_xor(nums)
        send = '262658160009' + data['id'] + "00" + xor + '0d'
        send = bytes().fromhex(send)
        log(data['id'] + '5016', '返回数据成功')
        coon1.setex("heartbeat" + data["id"], 100, "time")
        return send

    def ox5010(self, item):
        """
        给终端发送设置属性信息
        :param item:
        :return: 发送给终端的指令
        """
        imei = item['imei'].decode()
        x2 = item['data']
        data = json.loads(x2)
        count = data['count']
        if len(count) < 2:
            count = "0" + str(count)
        x3 = ''
        for key, value in data.items():

            if key != "count":
                id = key
                _type = value
                x3 += id + _type

        lens = hex((len(imei)
                    + len(count) + len(x3)) // 2)
        lens = str(lens[2:])
        if len(lens) == 1:
            lens = "000" + lens
        elif len(lens) == 2:
            lens = "00" + lens
        elif len(lens) == 3:
            lens = "0" + lens

        nums = '5010' + lens + imei + count + x3
        xor = change_xor(nums)
        send = '26265010' + str(lens) + imei + count + x3 + xor + '0d'
        log(imei + "5010", send + '平台下发垃圾桶数量及各垃圾桶设备')
        send = bytes().fromhex(send)
        QuoteFactory().w_dict[imei]['send'] = send
        coon1.setex("reply" + imei + item['protocol'].decode(), 3, "time")
        return send

    def ox5011(self, item):
        """
        发送给终端开门，关门信息
        :param item:
        :return: 处理过给终端的信息
        """

        imei = item['imei'].decode()
        data = json.loads(item['data'].decode())
        c_id = data['id']
        status = data['state']
        send = ''
        if len(c_id) < 2:
            c_id = '0' + c_id

        if status == '00':  # 关门
            nums = "5011000a" + imei + c_id + '00'
            xor = change_xor(nums)
            send = '26265011000a' + imei + c_id + '00' + xor + '0d'
            log(imei + '5011', '关门')
            coon0.hmset(imei + "_" + item["protocol"].decode(), {'mess': send, "imei": data["id"], "code": '1'})
            send = bytes().fromhex(send)

        elif status == '01':  # 开门
            nums = "5011000a" + imei + c_id + '01'
            xor = change_xor(nums)
            send = '26265011000a' + imei + c_id + "01" + xor + '0d'
            coon0.hmset(imei + "_" + item["protocol"].decode(), {'mess': send, "imei": data["id"], "code": '1'})

            log(imei + '5011', '开门')
            send = bytes().fromhex(send)
        coon1.setex("reply" + imei + item['protocol'].decode(), 3, "time", )
        QuoteFactory().w_dict[imei]['send'] = send
        return send

    def ox5012(self, data1):
        """
        接受终端上传数据，进行处理
        :param item:
        :return: 返回的成功失败信息
        """
        count = int(data1['data'][0:2], 16)
        id = data1['data'][2:4]
        type = data1['data'][4:6]
        c_weight = int(data1['data'][6:10], 16)
        z_weight = int(data1['data'][10:14], 16)
        state = data1['data'][14:16]
        try:
            token = QuoteFactory().w_dict['token' + data1["id"]]
            data = json.dumps(
                {"count": count, "id": id, "type": type, "z_weight": z_weight, "state": state, "c_weight": c_weight})
            coon1.hmset(data1['id'] + "_5012",
                        {"mess": data1["nums"], "imei": data1["id"], "protocol": "5012", "code": "0",
                         "data": data, "token": token})
        except Exception as ex:

            data = json.dumps(
                {"count": count, "id": id, "type": type, "z_weight": z_weight, "state": state, "c_weight": c_weight})
            coon1.hmset(data1['id'] + "_5012",
                        {"mess": data1["nums"], "imei": data1["id"], "protocol": "5012", "code": "0",
                         "data": data, })

        nums = "58120009" + data1['id'] + '00'
        xor = change_xor(nums)
        # 返回成功
        send = '262658120009' + data1['id'] + '00' + xor + '0d'
        log(data1['id'] + '5012', '上传数据')
        send = bytes().fromhex(send)
        return send

    def ox5013(self, item):
        """
        设置设备心跳定时回传时间间隔
        :param item:
        :return: 返回给终端指令
        """
        z_id = item['imei']
        data = item['data']
        x1 = data[0:2]
        x2 = data[2:4]
        nums = "5013" + "000A" + data['id'] + x1 + x2
        xor = change_xor(nums)
        send = '26265013000A' + z_id + x1 + x2 + xor + '0d'
        send = bytes().fromhex(send)
        QuoteFactory().w_dict[z_id]['send'] = send
        coon1.setex("heartbeat" + z_id, 15, "time", )

        return send

    def ox5014(self, item):
        """
        设置设备重启
        :param item:
        :return: 返回设备成功失败信息
        """

        # 设备重启 复位
        imei = item['imei']
        nums = '50140008' + imei
        xor = change_xor(nums)
        send = '262650140008' + item + xor + '0d'
        log(imei + '5014', '设备重启')
        coon0.hmset(imei + "_5014", {"type": "0", "code": "1", "mess": send})
        send = bytes().fromhex(send)
        QuoteFactory().w_dict[imei]['send'] = send
        coon1.setex("reply" + imei + item['protocol'], 3, "time")
        return send

    def ox5017(self, data):
        """
        接受终端设备故障信息
        :param data:
        :return: 成功失败信息
        """
        x1 = data['data'][0:2]
        x2 = data['data'][2:4]
        x3 = data['data'][4:6]
        x4 = data['data'][6:8]
        x5 = data['data'][8:10]
        x6 = data['data'][10:12]
        x7 = data['data'][12:14]
        x8 = data['data'][14:16]
        x9 = data['data'][16:18]
        x10 = data['data'][18:20]
        x11 = data['data'][20:22]
        x12 = data['data'][22:24]
        x13 = data['data'][24:26]
        x14 = data['data'][26:28]
        x15 = data['data'][28:30]
        x16 = data['data'][30:32]
        x17 = data['data'][32:34]
        x18 = data['data'][34:36]

        mess = json.dumps(
            {"x1": x1, "x2": x2, "x3": x3, "x4": x4, "x5": x5, "x6": x6, "x7": x7, "x8": x8, "x9": x9, "x10": x10,
             "x11": x11, "x12": x12, "x13": x13, "x14": x14, "x15": x15, "x16": x16, "x17": x17, "x18": x18})
        coon1.hmset(data['id'] + "_5017",
                    {"mess": data["nums"], "imei": data["id"], "protocol": "5017", "code": '0', "data": mess}
                    )
        # 返回成功信息
        nums = '58170009' + data['id'] + '00'
        xor = change_xor(nums)
        send = '262658170009' + data['id'] + '00' + xor + '0d'
        log(data['id'] + '5017', '垃圾站故障数据包')
        send = bytes().fromhex(send)
        return send

    def ox5018(self, data):
        """
        接受终端升级信息请求
        :param data:
        :return: 返回升级文件名称
        """
        try:
            coon1.hmset(data['id'] + "_5018", {"mess": data["nums"], "code": '0'})
        except Exception as ex:
            data = data['data']
            xor = change_xor("58180009" + data['id'] + data)
            send = '262658180009' + data['imei'] + data + xor + '0d'
            log(data['id'] + '5018', '查询文件版本返回升级文件名称')
            send = bytes().fromhex(send)
            return send

    def ox5019(self, data):
        """
        接受设备的心跳信息
        :param data:
        :return: 返回成功失败信息
        """
        # 主机到平台的心跳信息5分钟一次
        x1 = data['data'][0:2]
        x2 = data['data'][2:4]
        x3 = int(data['data'][4:8], 16) / 1000
        x4 = data['data'][8:10]
        x5 = data['data'][10:32]
        info = {}
        info["t1"] = x5[0:6]
        info["t2"] = x5[6:12]
        info["t3"] = x5[12:18]
        info["t4"] = x5[18:24]
        info["t5"] = x5[24:30]
        info["t6"] = x5[30:36]

        # 成功
        message = json.dumps({"x1": x1, "x2": x2, "x3": x3, "x4": x4, "x5": info
                              })
        coon1.hmset(data['id'] + '_5019',
                    {"status": "00", "protocol": "5019", "imei": data["id"], "mess": data["nums"], "code": '0',
                     "data": message})
        xor = change_xor('58190009' + data["id"] + '00')
        send = '262658190009' + data['id'] + x1 + xor + '0d'
        send = bytes().fromhex(send)
        id = data["id"]
        coon1.setex("heartbeat" + id, 130, "time")

        return send

    # def ox501a(self, data):
    #     """
    #     接受垃圾的重量信息
    #     :param data:
    #     :return:
    #     """
    #     x1 = data['data'][0:2]  # 垃圾桶id
    #     x2 = data['data'][2:4]  # 垃圾桶类别
    #     x2_state = data['data'][4:6]  # 垃圾桶状态
    #     z_weight = int(data['data'][6:10], 16)  # 垃圾桶总重量
    #     c_weight = int(data['data'][10:14], 16)  # 垃圾桶差量
    #     name = data["id"] + "_501a"
    #     info = json.dumps({"x1": x1, "x2": x2, "x2_state": x2_state, "z_weight": z_weight, "c_weight": c_weight})
    #     coon1.hmset(name, {"imei": data['id'], "mess": data["nums"], "code": "0", "protocol": "501a", "data": info})
    #     xor = change_xor("581a0009" + data['id'] + "00")
    #     send = "2626581a0009" + data['id'] + '00' + xor + "0d"
    #     return bytes().fromhex(send)

    def ox501b(self, data):

        """
        接受垃圾的重量信息
        :param data:
        :return:
        """
        x1 = data['data'][0:2]  # 垃圾桶id
        x2 = data['data'][2:4]  # 垃圾桶类别
        x2_state = data['data'][4:6]  # 垃圾桶状态
        z_weight = int(data['data'][6:10], 16)  # 垃圾桶总重量
        c_weight = int(data['data'][10:14], 16)  # 垃圾桶差量
        x6 = data["data"][14:16]
        name = data["id"] + "_501b"
        token = QuoteFactory().w_dict["worker" + data["id"]]
        info = json.dumps({"x1": x1, "x2": x2, "x2_state": x2_state, "z_weight": z_weight, "c_weight": c_weight,"x6 ":x6})
        coon1.hmset(name, {"token":token,"imei": data['id'], "mess": data["nums"], "code": "0", "protocol": "501b", "data": info})
        xor = change_xor("581b0009" + data['id'] + "00")
        send = "2626581b0009" + data['id'] + '00' + xor + "0d"
        return bytes().fromhex(send)


    def ox501c(self, item):

        """
        管理员开启清理门
        :param data:
        :return:
        """

        imei = item['imei'].decode()
        c_id = json.loads(item['data'].decode())
        c_id = c_id["id"]
        if len(c_id) < 2:
            c_id = '0' + c_id
        nums = "501c0009" + imei + c_id
        xor = change_xor(nums)
        send = '2626501c0009' + imei + c_id + xor + '0d'
        coon0.hmset(imei + "_" + item["protocol"].decode(), {'mess': send, "imei":imei, "code": '1',"protocol":item["protocol"].decode()})

        log(imei + '501c', '下发清理门开门指令')
        send = bytes().fromhex(send)
        coon1.setex("reply" + imei + item['protocol'].decode(), 3, "time", )
        QuoteFactory().w_dict[imei]['send'] = send
        return send


    def ox501d(self, data):

        """
        上报清理们的状态
        :param data:
        :return:
        """
        x1 = data['data'][0:2]  # 垃圾桶id
        x2 = data['data'][2:4]  # 开门状态

        name = data["id"] + "_501b"
        info = json.dumps({"x1": x1, "x2": x2, })
        coon1.hmset(name, {"imei": data['id'], "mess": data["nums"], "code": "0", "protocol": "501d", "data": info})
        xor = change_xor("581b0009" + data['id'] + "00")
        send = "2626581d0009" + data['id'] + '00' + xor + "0d"
        log(data['id'] + '_581c', '接受到501d的状态')
        return bytes().fromhex(send)

    def ox581c(self, data):
        """
        开启清理们终端应答
        :param data:
        :return:
        """
        x1 = data['data'][0:2]
        if x1 == '00':
            coon1.hmset(data['id'] + '_581c',
                        {"mess": data["nums"], "imei": data['id'], 'data': x1, "protocol": "581c", "code": '0'})
            log(data['id'] + '_581c', '清理门开门成功')
        else:
            coon1.hmset(data['id'] + '_581c',
                        {"mess": data["nums"], "imei": data['id'], 'data': x1, "protocol": "5810", "code": '0'})
            log(data['id'] + '_581c', '清理门开门失败')

    def ox5810(self, data):
        """
        接受终端应答成功，失败
        :param data:
        :return:
        """

        count = data['data'][0:2]
        li = {}
        a = 2
        b = 4
        for i in range(int(count, 16)):
            li[data['data'][a:b]] = data['data'][a + 2:b + 2]
            a += 4
            b += 4
        li = json.dumps(li)
        coon1.hmset(data['id'] + '_5810',
                    {"mess": data["nums"], "imei": data['id'], 'data': li, "protocol": "5810", "code": '0'})
        log(data['id'] + '_5810', '接受到5810')

    def ox5811(self, data):
        # 5011终端应答
        # z_id = data['data'][0:2]
        x1 = data['data'][0:2]
        x2 = data['data'][2:4]
        x3 = data['data'][4:6]
        name = data['id'] + '_5811'
        # self.redis_dict.pop([data['id'] + "_5011"])
        if x2 == '00' and x3 == '00':  # 关门成功,

            mess = json.dumps({"status": '00', "action": "00"})
            coon1.hmset(name, {"data": mess, "mess": data["nums"], "imei": data["id"], "protocol": '5811',
                               "code": '0'})
            log(data['id'], '关门成功')
            # 删除该redis值

        elif x2 == '01' and x3 == '00':  # 开门成功
            mess = json.dumps({"status": "00", "action": "00"})
            coon1.hmset(name, {"mess": data["nums"], "protocol": "5811", "imei": data["id"], "data": mess,
                               "code": '0'})
            log(data['id'], '开门成功')
        elif x2 == '00' and x3 == '01':  # 关门失败
            mess = json.dumps({"status": "00", "action": "01", })
            coon1.hmset(name, {"mess": data["nums"], "protocol": "5811", "imei": data["id"], "data": mess,
                               "code": '0'})
            log(data['id'], '关门失败')
        elif x2 == '01' and x3 == '01':  # 开门失败
            mess = json.dumps({"status": "00", "action": "00"})
            coon1.hmset(name, {"mess": data["nums"], "protocol": "5811", "imei": data["id"], "data": mess,
                               "code": '0'})
            log(data['id'], '开门失败')

    def ox5813(self, data):
        x1 = data['data']
        if x1 == '00':
            mess = json.dumps({'status': x1})
            coon1.hmset(data['id'] + '_5813',
                        {"mess": data["nums"], "protocol": "ox5813", "imei": data["id"], "data": mess, "code": '0'})

        else:
            mess = json.dumps({'status': x1})
            coon1.hmset(data['id'] + '_5813',
                        {"mess": data["nums"], "protocol": "ox5813", "imei": data["id"], "data": mess, "code": '0'})

    def ox5814(self, data):
        x1 = data['data']
        self.redis_dict.pop([data['id'] + "_5014"])
        if x1 == '00':
            mess = json.dumps({'status': x1})
            coon1.hmset(data['id'] + '_5814',
                        {"mess": data["nums"], "protocol": "ox5814", "imei": data["id"], "data": mess, "code": '0'})

        else:
            mess = json.dumps({'status': x1})
            coon1.hmset(data['id'] + '_5814',
                        {"mess": data["nums"], "protocol": "ox5814", "imei": data["id"], "data": mess, "code": '0'})


reactor.listenTCP(9030, QuoteFactory())
log('9030', "服务已经启动")
reactor.run()
