import base64
import random
from settings import proxies
class ProxyMiddleware(object):
    def process_request(self, request, spider):
        proxy = random.choice(proxies)
        proxy = {'http': proxy, 'https': proxy}
        request.meta['proxies'] = proxy

