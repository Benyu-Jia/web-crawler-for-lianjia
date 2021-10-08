import requests
import multiprocessing
from selenium import webdriver
from bs4 import BeautifulSoup as bsp
from pymongo import MongoClient
from tqdm import tqdm
import time


# config info
db_ip = 'localhost'
db_port = 27017
db_name = "ST1533"
db_col = 'bj'
home_url = 'https://bj.lianjia.com'
wait_time = 0.5
process_num = 6


# rotating proxy
#input is where to input IPs. since I am using paid IPs, I just leave a templete here
input = """ip:host:username:password
ip:host:username:password
ip:host:username:password
ip:host:username:password
ip:host:username:password
""" 


# store proxies and rotating number
class proxy_num():
    value = 0
    proxies = list(input.split('\n'))


# function return a rotating proxy
def get_proxy():
    if proxy_num.value >= len(proxy_num.proxies):
        proxy_num.value -= len(proxy_num.proxies)
    info = proxy_num.proxies[proxy_num.value].split(":")
    temp = info[2] + ":" + info[3] + "@" + info[0] + ":" + info[1]
    proxy = {"http": temp}
    proxy_num.value += 1
    return proxy


# function export extension for chrome proxy
def create_proxyauth_extension(proxy_host, proxy_port,
                               proxy_username, proxy_password,
                               scheme='http', plugin_path=None):
    import string
    import zipfile

    if plugin_path is None:
        plugin_path = 'd:/vimm_chrome_proxyauth_plugin.zip'

    manifest_json = """
    {
        "version": "1.0.0",
        "manifest_version": 2,
        "name": "Chrome Proxy",
        "permissions": [
            "proxy",
            "tabs",
            "unlimitedStorage",
            "storage",
            "<all_urls>",
            "webRequest",
            "webRequestBlocking"
        ],
        "background": {
            "scripts": ["background.js"]
        },
        "minimum_chrome_version":"22.0.0"
    }
    """

    background_js = string.Template(
    """
    var config = {
            mode: "fixed_servers",
            rules: {
              singleProxy: {
                scheme: "${scheme}",
                host: "${host}",
                port: parseInt(${port})
              },
              bypassList: ["foobar.com"]
            }
          };

    chrome.proxy.settings.set({value: config, scope: "regular"}, function() {});

    function callbackFn(details) {
        return {
            authCredentials: {
                username: "${username}",
                password: "${password}"
            }
        };
    }

    chrome.webRequest.onAuthRequired.addListener(
                callbackFn,
                {urls: ["<all_urls>"]},
                ['blocking']
    );
    """
    ).substitute(
        host=proxy_host,
        port=proxy_port,
        username=proxy_username,
        password=proxy_password,
        scheme=scheme,
    )
    with zipfile.ZipFile(plugin_path, 'w') as zp:
        zp.writestr("manifest.json", manifest_json)
        zp.writestr("background.js", background_js)

    return plugin_path


#############################################################################
# function for fetching page
# input: url: string (url of the web page)
#        option: str (method for fetch web page: requests or selenium)
# return: soup: beautifulsoup (processed html data)
# error handle: if error happens, the function will print error url and return None
##############################################################################
def fetch_page(url: str, option: str):
    if option == 'requests':
        try:
            # config request
            proxy = get_proxy()
            requests.adapters.DEFAULT_RETRIES = 15
            s = requests.session
            s.keep_alive = False
            response = requests.get(url, proxies=proxy)
            soup = bsp(response.text, 'html.parser')
            return soup
        except:
            print("error fetching page: " + url)
            pass
            return None
    if option == 'selenium':
        try:
            if proxy_num.value >= len(proxy_num.proxies):
                proxy_num.value -= len(proxy_num.proxies)
            info = proxy_num.proxies[proxy_num.value].split(":")
            proxy_num.value += 1
            proxyauth_plugin_path = create_proxyauth_extension(
                proxy_host=info[0],
                proxy_port=info[1],
                proxy_username=info[2],
                proxy_password=info[3]
            )
            chrome_options = webdriver.ChromeOptions()
            chrome_options.add_extension(proxyauth_plugin_path)
            prefs = {'profile.managed_default_content_settings.images': 2}
            chrome_options.add_experimental_option('prefs',prefs)
            chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)
            soup = bsp(driver.page_source, 'html.parser')
            driver.quit()
            time.sleep(0.1)
            return soup
        except Exception as e:
            print(e)
            # print("error fetching page: " + url)
            # pass
            # return None
    try:
        # config request
        proxy = get_proxy()
        requests.adapters.DEFAULT_RETRIES = 15
        s = requests.session
        s.keep_alive = False
        response = requests.get(url, proxies=proxy)
        soup = bsp(response.text, 'html.parser')
        return soup
    except:
        # print("error fetching page: " + url)
        pass
        return None


######################################################################
# function for extract info from html data
# input: soup: beautifulsoup (processed html data)
# return: examples: list of dict (extracted info with key and value)
# error handle: if error happens, it will print error extract info
######################################################################
def extract_info(soup: bsp, region: str):
    examples = []
    for house in soup.findAll(class_="info clear"):
        try: 
            example = {}
            example['region'] = region
            href = house.find(class_="title").a["href"]
            example['href'] = href
            # print(href)
            houseCode = house.find(class_="title").a["data-housecode"]
            example['houseCode'] = houseCode
            # print(houseCode)
            houseType = house.find(class_="title").a["data-el"]
            example['houseType'] = houseType
            # print(houseType)
            housePosi = house.find(class_="positionInfo").text
            # print(housePosi)
            houseInfo = house.find(class_="houseInfo").text
            houseInfo = houseInfo.replace(" ", "")
            infos = houseInfo.split("|")
            attr = ["布局", "大小", "朝向", "装修", "楼层", "建筑时间", "类型"]
            for i in range(len(infos)):
                if i >= 7:
                    example[str(i)] = infos[i]
                    continue
                example[attr[i]] = infos[i]
            # print(houseInfo)
            houseTag = house.find(class_="tag").text
            example['houseTag'] = houseTag
            # print(houseTag)
            housePrice = house.find(class_="unitPrice")["data-price"]
            example['housePrice'] = housePrice
            # print(housePrice)
            examples.append(example)
        except:
            # print("error extract info")
            pass
    return examples


########################################################
# function for store info into mongodb
# input: collection: str (name of mongodb collection)
#        examples: list of dict (extracted info)
########################################################
def store_info(collection: str, examples: list):
    try:
        client = MongoClient(db_ip, db_port)
        mydb = client[db_name]
        mycol = mydb[collection]
        for example in examples:
            mycol.insert_one(example)
    except:
        # print("error store info")
        pass


# combanation of fetch info, extract info, and store info
# using try and except to keep running if meet error
def fetch_info(url: str, col: str, region: str):
    try:
        soup = fetch_page(url, "requests")
        examples = extract_info(soup, region)
        store_info(col, examples)
    except:
        # print("error fetching info")
        pass


# function for multiprocessing
def run_process(region: str, price: str):
    url = home_url + region + price
    pbar = tqdm(desc=url, position=1)
    max_page = 0
    soup = fetch_page(url, 'selenium')
    for child in soup.find(class_="page-box fr").div.children:
        try:
            max_page = max(max_page, int(child.text))
        except:
            pass 
    if max_page == 0:
        fetch_info(url, db_col, region)
        pbar.update(1)
        time.sleep(wait_time)
    else:
        temp_urls = []
        for pg in range(max_page):
            temp_urls.append(home_url + region + 'pg' + str(pg + 1) + price)
        for temp_url in temp_urls:
            fetch_info(temp_url, db_col, region)
            pbar.update(1)
            time.sleep(wait_time)


# main function
if __name__ == '__main__':
    # fetch home page
    temp_url = home_url + "/ershoufang/"
    soup = fetch_page(temp_url, 'requests')

    # fetch region info
    region_href = []
    for child in soup.find(attrs={"data-role": "ershoufang"}).div.children:
        if child != '\n':
            region_href.append(child['href'])

    # fetch price info
    price_href = ['p1/', 'p2/', 'p3/', 'p4/', 'p5/', 'p6/', 'p7/']
   
    # 
    with multiprocessing.Pool(process_num) as pool:
        paras = []
        for temp1 in region_href:
            for temp2 in price_href:
                paras.append((temp1, temp2, ))
        for para in paras:
            res = pool.apply_async(run_process, para)
        pool .close()
        pool.join()
