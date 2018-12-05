from twisted.internet.protocol import Factory, connectionDone
from twisted.internet import reactor, protocol
import re
import logging


class QuoteProtocol(protocol.Protocol):
    def __init__(self, factory):
        self.logger = logging.getLogger('mylogger')
        self.logger.setLevel(logging.INFO)
        self.fount = Route()
        self.factory = factory
        self.route = {
            '5020': self.fount.ox5020,
            '5016': self.fount.ox5016,
            '5010': self.fount.ox5010,
            '5011': self.fount.ox5011,
            '5012': self.fount.ox5012,
            '5013': self.fount.ox5013,
            '5014': self.fount.ox5014,
            '5017': self.fount.ox5017,
            '5018': self.fount.ox5018,
            '5019': self.fount.ox5019,
            '5810': self.fount.ox5810,
            '5811': self.fount.ox5811
        }

    # 建立连接后的回调函数
    def connectionMade(self):
        self.factory.numConnections += 1
        fh = logging.FileHandler('data.log')
        fh.setLevel(logging.INFO)
        ch = logging.StreamHandler()
        ch.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        ch.setFormatter(formatter)
        self.logger.addHandler(fh)
        self.logger.addHandler(ch)
        self.logger.info('已经连接')

    def dataReceived(self, data):
        # 接收到数据后判断是终端请求还是前端请求
        self.logger.info(data)
        # self._data_buffer+=data
        # print(self._data_buffer)
        data = data.decode()

        # print(w)

        if re.match('^2626.*990d$', data):
            # 如果以990d结尾就是终端请求
            w = self.factory.wdict

            id = data[12:27]

            w[id] = {}

            w[id]['ip'] = self.transport.getPeer().host
            w[id]['title'] = data[0:4]
            w[id]['protocol'] = data[4:8]
            w[id]['len'] = (data[8:12])
            w[id]['id'] = str("0") + data[12:27]

            w[id]['end'] = data[-2:]
            num = len(data) - 4
            w[id]['data'] = data[27:num]

            # 数据验证
            # if q['len'] == len(data) and q['end'] == '0d':

            w[id]['tr'] = self.transport
            self.route[w[id]['protocol']](w[id])
            w[id]['tr'].write(self.getQuote())
            w[id]['tr'].write(self.fount.send1.encode())

            # print(self.factory.wdict)
        elif re.match('^qd', data):
            # 前段传过来的请求,协议号,主机id,内容
            # qd50113565390403186330100

            z_id = self.route[data[2:6]](data)
            # print(w)
            self.factory.wdict[z_id]['tr'].write(self.getQuote())

    # 断开连接后的回调函数
    def connectionLost(self, reason=connectionDone):
        self.factory.numConnections -= 1
        print('断开链接')

    def getQuote(self):
        return self.fount.send.encode()

    def updateQuote(self, quote):
        # quote = quote.decode()

        self.factory.quote = quote


class QuoteFactory(Factory):
    numConnections = 0
    wdict = {}
    ff = ''

    # 数据接收后放在在quote中
    def __init__(self, quote=None):
        self.quote = quote or str('数据存储').encode("utf-8")

    def buildProtocol(self, addr):
        return QuoteProtocol(self)


class Route():
    def __init__(self):

        self.send = ''
        self.send1 = ''

    def ox5016(self, data):
        x1 = data['data'][0:16]
        x2 = data['data'][16:19]
        x3 = data['data'][19:22]
        x4 = data['data'][22:25]
        x5 = data['data'][25:28]
        self.send = '262658160009' + data['id'] + x1 + '990d'
        self.ox5010()

    def ox5020(self, data):
        # 接收前段的数据  进行解析
        z_id = data[6:21]
        x1 = data[21:23]
        a = 23
        b = 27
        x2 = ''
        for i in range(int(x1)):
            x2 += data[a:b]
            a += 4
            b += 4

        self.ff = "262650100a" + z_id + x1 + x2 + '990d'
        return z_id

    def ox5010(self):
        # qd5020356539040318633010102
        self.send1 = self.ff

    def ox5011(self, data):
        # 接收一条前端的数据,有设备id,开关信息
        # 测试数据qd50113565390403186330100

        z_id = data[6:21]
        c_id = data[21:23]
        switch = data[23:25]
        senddata = "26265011000c" + c_id + switch + "99" + "0d"
        self.send = senddata
        return z_id

    def ox5012(self, data):
        group = int((int(data['len']) - 8 - 1) / 10)
        a = 2
        b = 12
        li = []
        for i in range(group):
            info = {}
            sd = data['data'][a:b]
            info['id'] = sd[0:2]
            info['type'] = sd[2:4]
            info['weight'] = sd[4:8]
            info['status'] = sd[8:10]
            a += 10
            b += 10
            li.append(info)
        # 2626501200343565390403186330201010005000202000300990d
        # 返回成功
        self.send = '262658120009' + data['id'] + str(group) + '00990d'

    def ox5013(self, data):
        # 设置设备心跳定时回传时间间隔
        # qd50133565390403186330101
        z_id = data[6:21]
        x1 = data[21:23]
        x2 = data[23:25]
        self.send = '26265013000A' + z_id + x1 + x2 + '990d'
        return z_id

    def ox5014(self, data):
        # 设备重启 复位
        z_id = data[6:21]

        self.send = '262650140008' + data['id'] + '990d'
        return z_id

    def ox5017(self, data):
        # 26265017000a3565390403186330100990d
        x1 = data['data'][27:29]
        x2 = data['data'][29:31]
        # 返回成功信息
        self.send = '262658170009' + data['id'] + '00' + '990d'

    def ox5018(self, data):
        z_id = data['data'][12:28]
        # 262650180009356539040318633990d
        # 查询文件版本返回升级文件名称
        x1 = 'JKCC_V1.001'
        self.send = '262658180009' + z_id + x1 + '990d'

    def ox5019(self, data):
        # 主机到平台的心跳信息5分钟一次
        # 262650191235653904031863305010101010501990d
        z_id = data['data'][12:28]
        x1 = '00'  # 成功
        self.send = '262658190009' + z_id + x1 + '990d'

    def ox5810(self, data):
        # 0x5010终端应答
        z_id = data['data'][12:28]
        x1 = data['data'][28:30]

    def ox5811(self, data):
        # 5011终端应答
        z_id = data['data'][12:28]
        x1 = data['data'][28:30]
        x2 = data['data'][30:]
        pass


reactor.listenTCP(9000, QuoteFactory())

reactor.run()
