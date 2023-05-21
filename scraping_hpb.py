import csv
import sys
import logging
import datetime
from time import sleep
from pathlib import Path
from multiprocessing import Pool
from tqdm import tqdm
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

logger = logging.getLogger(__name__)

# ログを保存するディレクトリが無ければ作成する。
log_save_dir = "./log/"
if not Path(log_save_dir).exists():
    Path(log_save_dir).mkdir(parents=True)

# ログ周りの設定
logger.setLevel(10)

formatter = logging.Formatter('時間:%(asctime)s 行:%(lineno)d ログレベル:%(levelname)s メッセージ:%(message)s')

sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

# ログファイルの追加設定(ファイル出力)
start_time = datetime.datetime.today().strftime('%Y-%m-%d_%H:%M:%Sstart')
fh = logging.FileHandler(f'./log/{start_time}.log')
logger.addHandler(fh)
fh.setFormatter(formatter)

def get_all_kuchikomi(area_url):
    """指定したエリアのすべてのサロンの情報を取得する。

    Parameters
    ----------
    area_url : str
        「~県トップ」以下の各エリアのurlを指定する。
    
    Notes
    -----
    返り値は無し。
    """
    
    driver = webdriver_options()
    
    driver.get(area_url)
    sleep(2)
    prefecture = driver.find_element_by_css_selector('#preContents > ol > li:nth-child(4) > a').text.replace('トップ', '')
    area = driver.find_element_by_css_selector('#preContents > ol > li:nth-child(5)').text.replace(' ', '').replace('>', '')
    logger.info(f'{prefecture}_{area}の口コミを取得開始')

    
    # ファイルを保存するディレクトリが無ければ作成する。
    save_dir = "./data/"
    if not Path(save_dir).exists():
        Path(save_dir).mkdir(parents=True)

    # pathを作成する。
    save_file_path = save_dir + f'{prefecture}_{area}.csv'

    # 新規ファイル作成、ヘッダのみ書き込み
    make_new_save_file(save_file_path=save_file_path)
              
    page = 1
    while True:
        
        # サロン情報の大枠を取得する。(現在の「page」のすべてのサロンの情報【aタグ→サロン名とサロンurlを取得】)
        all_salon = driver.find_elements_by_css_selector(".slnName")
        # sys.exit()
        all_salon_a =  [salon.find_element_by_tag_name("a") for salon in all_salon]
        all_salon_a_text = [salon_a.text for salon_a in all_salon_a]
        all_salon_a_url = [salon_a.get_attribute("href") for salon_a in all_salon_a]
        all_salon_info = driver.find_elements_by_css_selector('div.slnInfo')

        all_salon_access = []
        for salon_info in all_salon_info:
            try:
                salon_access = salon_info.find_element_by_tag_name('dd.access').text
                all_salon_access.append(salon_access)
            except Exception:
                all_salon_access.append('')
        
        all_salon_cut_price = []
        for salon_info in all_salon_info:
            try:
                salon_cut_price = salon_info.find_element_by_tag_name('dd.price').text
                all_salon_cut_price.append(salon_cut_price)
            except Exception:
                all_salon_cut_price.append('')
        
        all_salon_cut_price = []
        for salon_info in all_salon_info:
            try:
                salon_cut_price = salon_info.find_element_by_tag_name('dd.price').text
                all_salon_cut_price.append(salon_cut_price)
            except Exception:
                all_salon_cut_price.append('')        
        
        all_salon_seat = []
        for salon_info in all_salon_info:
            try:
                salon_seat = salon_info.find_element_by_tag_name('dd.seat').text
                all_salon_seat.append(salon_seat)
            except Exception:
                all_salon_seat.append('')        
                
        all_salon_blog_count = []
        for salon_info in all_salon_info:
            try:
                salon_blog_all_salon_blog_count = salon_info.find_element_by_tag_name('dd.blog').text
                all_salon_blog_count.append(salon_blog_all_salon_blog_count)
            except Exception:
                all_salon_blog_count.append('')        
                
        all_salon_review_count = []
        for salon_info in all_salon_info:
            try:
                salon_blog_all_salon_review_count = salon_info.find_element_by_tag_name('dd.message').text
                all_salon_review_count.append(salon_blog_all_salon_review_count)
            except Exception:
                all_salon_review_count.append('')        
        
        # クーポン上位三件を取得する。
        all_salon_coupon_frame = []
        for salon_info in all_salon_info:
            try:
                salon_blog_all_salon_coupon_frame = salon_info.find_element_by_tag_name('ul.slnCouponList')
                all_salon_coupon_frame.append(salon_blog_all_salon_coupon_frame)
            except Exception:
                all_salon_coupon_frame.append('')
        
        one_coupon_list = []
        two_coupon_list = []
        three_coupon_list = []
        for salon_coupon_frame in all_salon_coupon_frame:
            _one_coupon_dict = {}
            _two_coupon_dict = {}
            _three_coupon_dict = {}
            # クーポンの枠がなかった時の処理
            if salon_coupon_frame == '':
                
                _one_coupon_dict['coupon_name'] = ''
                _one_coupon_dict['coupon_price'] = ''
                one_coupon_list.append(_one_coupon_dict)
                
                _two_coupon_dict['coupon_name'] = ''
                _two_coupon_dict['coupon_price'] = ''
                two_coupon_list.append(_two_coupon_dict)
                
                _three_coupon_dict['coupon_name'] = ''
                _three_coupon_dict['coupon_price'] = ''
                three_coupon_list.append(_three_coupon_dict)
                
                continue
            
            try:
                _one_coupon_dict['coupon_name'] = salon_coupon_frame.find_elements_by_tag_name('a.slnCouponLink')[0].text
                _one_coupon_dict['coupon_price'] = salon_coupon_frame.find_elements_by_tag_name('p.slnCouponPrice')[0].text
                one_coupon_list.append(_one_coupon_dict)
            except Exception:
                _one_coupon_dict['coupon_name'] = ''
                _one_coupon_dict['coupon_price'] = ''
                one_coupon_list.append(_one_coupon_dict)
                
            try:
                _two_coupon_dict['coupon_name'] = salon_coupon_frame.find_elements_by_tag_name('a.slnCouponLink')[1].text
                _two_coupon_dict['coupon_price'] = salon_coupon_frame.find_elements_by_tag_name('p.slnCouponPrice')[1].text
                two_coupon_list.append(_two_coupon_dict)
            except Exception:
                _two_coupon_dict['coupon_name'] = ''
                _two_coupon_dict['coupon_price'] = ''
                two_coupon_list.append(_two_coupon_dict)
                
            try:
                _three_coupon_dict['coupon_name'] = salon_coupon_frame.find_elements_by_tag_name('a.slnCouponLink')[2].text
                _three_coupon_dict['coupon_price'] = salon_coupon_frame.find_elements_by_tag_name('p.slnCouponPrice')[2].text
                three_coupon_list.append(_three_coupon_dict)
            except Exception:
                _three_coupon_dict['coupon_name'] = ''
                _three_coupon_dict['coupon_price'] = ''
                three_coupon_list.append(_three_coupon_dict)
                    
        print(f'サロン一覧[{page}]ページ目')
        
        for salon_num in range(len(all_salon_a)):
            driver.get(all_salon_a_url[salon_num])
            sleep(2)
            
            # リスト内の情報を取得
            salon_name = all_salon_a_text[salon_num]
            salon_url = all_salon_a_url[salon_num]
            
            salon_address =\
                driver.find_element_by_css_selector('#mainContents > div.detailHeader.cFix.pr > div > div.pL10.oh.hMin120 > div > div > ul > li:nth-child(1)').text
            
            salon_access = all_salon_access[salon_num]
            salon_cut_price = all_salon_cut_price[salon_num]
            salon_seat = all_salon_seat[salon_num]
            salon_blog_count = all_salon_blog_count[salon_num]
            salon_review_count = all_salon_review_count[salon_num]
            salon_one_coupon_name = one_coupon_list[salon_num]['coupon_name']
            salon_one_coupon_price = one_coupon_list[salon_num]['coupon_price']
            salon_two_coupon_name = two_coupon_list[salon_num]['coupon_name']
            salon_two_coupon_price = two_coupon_list[salon_num]['coupon_price']
            salon_three_coupon_name = three_coupon_list[salon_num]['coupon_name']
            salon_three_coupon_price = three_coupon_list[salon_num]['coupon_price']
            
            # 平均予約金額を取得する。
            # 初回
            try:
                first_time_money =\
                    driver.find_element_by_css_selector\
                        ('#mainContents > div.mT30 > div.mT20 > table.averageCostTbl.mT15 > tbody > tr > td.jscAveragePriceFirstArea').text

            except Exception:
                first_time_money = ''
            
            # 2回目
            try:
                repeat_money =\
                    driver.find_element_by_css_selector\
                        ('#mainContents > div.mT30 > div.mT20 > table.averageCostTbl.mT15 > tbody > tr >  td.jscAveragePriceSecondOnwardsArea').text
            
            except Exception:
                repeat_money = ''
            
            # 次のtry:except文で「クーポンメニュー」サロンにアクセスした際のエラーを回避
            try:
                # クーポン&メニュー欄にアクセス
                salon_coupon_menu_url = driver.find_element_by_css_selector("a.couponAndMenu").get_attribute('href')
                driver.get(salon_coupon_menu_url)
                sleep(2)
                
                # クーポン数を取得
                try:
                    salon_coupon_count = driver.find_element_by_css_selector('#mainContents > div:nth-child(2) > div.preListHead.mT20 > div > p.couponResultMessage > span.mR10 > span').text
                except Exception:
                    salon_coupon_count = ''
                
                # メニュー数を取得
                try:
                    salon_menu_count = driver.find_element_by_css_selector('#mainContents > div:nth-child(2) > div.preListHead.mT20 > div > p.couponResultMessage > span.numberOfResult').text
                except Exception:
                    salon_menu_count = ''
                
                # 戻る
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            except Exception:
                salon_coupon_count = ''
                salon_menu_count = ''
                # 戻る。
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
                
            # 次のtry:except文で「メニュー」サロンにアクセスした際のエラーを回避
            try:
                # クーポン&メニュー欄にアクセス
                salon_coupon_menu_url = driver.find_element_by_css_selector("a.salonMenu").get_attribute('href')
                driver.get(salon_coupon_menu_url)
                sleep(2)
                
                # メニュー数を取得
                try:
                    salon_menu_count = driver.find_element_by_css_selector('#mainContents > div:nth-child(2) > div.preListHead.mT20 > div > p.couponResultMessage > span.numberOfResult').text
                except Exception:
                    salon_menu_count = ''
                
                # 戻る
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            except Exception:
                
                if salon_coupon_count:
                    pass
                else:
                   salon_coupon_count = '' 
                
                if salon_menu_count:
                    pass
                else:
                    salon_menu_count = ''
                # 戻る。
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
                
            # 次のtry文でスタイリストにアクセスした時のエラーを回避
            try:
                salon_stylist_url = driver.find_element_by_css_selector("a.salonMenuTab.stylist").get_attribute('href')
                driver.get(salon_stylist_url)
                sleep(2)
                number_of_staff = driver.find_element_by_css_selector('#mainContents > div.mT20 > div.mT15.pL15 > div > div > p:nth-child(1) > span').text
                
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            except Exception:
                number_of_staff = ''
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            
            # 次のtry:except文で「スタイル」にアクセスした際のエラーを回避
            try:
                salon_style_url = driver.find_element_by_css_selector("a.salonMenuTab.style").get_attribute('href')
                driver.get(salon_style_url)
                sleep(2)
                
                hair_style_count = driver.find_element_by_css_selector('#mainContents > div.mT20 > div.pH10.mT25.pr > p:nth-child(1) > span').text
                
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            except Exception:
                hair_style_count = ''
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            
            # 次のtry:except文で「ブログ」にアクセスした際のエラーを回避
            try:
                salon_blog_url = driver.find_element_by_css_selector("a.salonMenuTab.blog").get_attribute('href')
                driver.get(salon_blog_url)
                sleep(2)
                
                hair_blog_count = driver.find_element_by_css_selector('#jsiBlogContents > div.blogMainCntListWrap > div.preList > div > div > p:nth-child(1) > span').text
                
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            except Exception:
                hair_blog_count = ''
                driver.get(all_salon_a_url[salon_num])
                sleep(2)
            
            print(f'No.{salon_num+1} サロン名: {salon_name} URL: {salon_url} の口コミを取得中')
            # 次のtry:except文で「口コミ欄がない」サロンにアクセスした際のエラーを回避
            try:
                salon_kuchikomi = driver.find_element_by_css_selector("li.kuchikomi")
                salon_kuchikomi_url = salon_kuchikomi.find_element_by_tag_name("a").get_attribute("href")
                driver.get(salon_kuchikomi_url)
                sleep(2)
                
                # 口コミ平均の枠を指定。
                try:
                    avarage_score_frame = driver.find_element_by_css_selector('#mainContents > div:nth-child(2) > div > div')
                    all_avarage_score = avarage_score_frame.find_element_by_tag_name('dd.reviewRatingMeanScore.jscReviewRatingMeanScore').text
                    atmosphere_avarage = avarage_score_frame.find_element_by_tag_name('ul.reviewRatingDetailList > li:nth-child(1) > dl > dd').text
                    attitude_avarage = avarage_score_frame.find_element_by_tag_name('ul.reviewRatingDetailList > li:nth-child(2) > dl > dd').text
                    quality_avarage = avarage_score_frame.find_element_by_tag_name('ul.reviewRatingDetailList > li:nth-child(3) > dl > dd').text
                    price_and_menu_avarage = avarage_score_frame.find_element_by_tag_name('ul.reviewRatingDetailList > li:nth-child(4) > dl > dd').text
                except Exception:
                    all_avarage_score = ''
                    atmosphere_avarage = ''
                    attitude_avarage = ''
                    quality_avarage = ''
                    price_and_menu_avarage = ''
                #口コミページから口コミへのアクセスを開始する。
                i = 1
                while True:
                        print(f'{salon_name}の口コミ({i}ページ目)を取得中')
                        # 以下の関数で1ページ分の口コミを取得する。
                            # 口コミの枠を取得。
                        kuchikomis = driver.find_elements_by_css_selector("li.reportCassette.mT30")
                        
                        
                        for kuchikomi in kuchikomis:
                            # (自分用のメモ)子要素の検索「.//タグ名」、「//タグ名」だと（「．」がないと）ルートから辿って一番最初のタグ名ににアクセス。
                            # (自分用のメモ)cssセレクタ「nth-child」はサポートされていないので「nth-of-type」に書き換える。

                            customer_name = kuchikomi.find_element_by_css_selector("span.b").text
                            gender_age = kuchikomi.find_element_by_css_selector("span.mL5.fs10.fgGray").text
                            posted_date = kuchikomi.find_element_by_tag_name('div.reportHeader > div.shopInfo.reportTitle.cFix > div.fr > p.fs10.fgGray').text
                            allover_review = kuchikomi.find_element_by_css_selector("span.mL5.mR10.fgPink").text
                            atmosphere = kuchikomi.find_element_by_css_selector("ul > li:nth-of-type(2) > span.mL10.fgPink.b").text
                            attitude = kuchikomi.find_element_by_css_selector("ul > li:nth-of-type(3) > span.mL10.fgPink.b").text
                            quality = kuchikomi.find_element_by_css_selector("ul > li:nth-of-type(4) > span.mL10.fgPink.b").text
                            price_and_menu = kuchikomi.find_element_by_css_selector("ul > li:nth-of-type(5) > span.mL10.fgPink.b").text
                            detail = kuchikomi.find_element_by_css_selector("p.mT10.wwbw").text
                            selected_coupon = kuchikomi.find_element_by_css_selector('dl.mT25 > dd.oh.zoom1 > p').text
                            kind_of_menu = kuchikomi.find_element_by_css_selector('dl.mT25 > dd.oh.zoom1 > p.fs10').text

                            # コメントへの返信の有無を入れる。
                            try:
                                kuchikomi.find_element_by_css_selector('div.mT20.mH10.pV5.pH9.bdGray')
                                response = '有'
                            except Exception:
                                response = '無'
                            
                            _csv_row = [prefecture, area, salon_name, salon_url, salon_address, salon_access, salon_cut_price, salon_seat, salon_blog_count, salon_review_count,
                                        number_of_staff, salon_one_coupon_name, salon_one_coupon_price, salon_two_coupon_name,
                                        salon_two_coupon_price, salon_three_coupon_name, salon_three_coupon_price,
                                        first_time_money, repeat_money, salon_coupon_count, salon_menu_count, hair_style_count,
                                        hair_blog_count, all_avarage_score, atmosphere_avarage, attitude_avarage, quality_avarage, price_and_menu_avarage,
                                        customer_name, gender_age, posted_date, allover_review, atmosphere, attitude, quality, price_and_menu,
                                        detail, selected_coupon, kind_of_menu, response]
                            
                            with open(save_file_path, "a", encoding='UTF-8', errors="ignore") as f:
                                writer = csv.writer(f, lineterminator='\n')
                                writer.writerow(_csv_row)
                    
                        # 口コミの次のページへ遷移
                        try:
                            next_button(driver=driver)
                            i += 1
                        except Exception:
                            print(f'{salon_name}の口コミ取得完了')
                            break
            
            except Exception:
                all_avarage_score = ''
                atmosphere_avarage = ''
                attitude_avarage = ''
                quality_avarage = ''
                price_and_menu_avarage = ''
                customer_name=''
                gender_age=''
                posted_date=''
                allover_review=''
                atmosphere=''
                attitude=''
                quality=''
                price_and_menu=''
                detail=''
                selected_coupon=''
                kind_of_menu=''
                response=''
                
                
                _csv_row = [prefecture, area, salon_name, salon_url, salon_address, salon_access, salon_cut_price, salon_seat, salon_blog_count, salon_review_count,
                            number_of_staff, salon_one_coupon_name, salon_one_coupon_price, salon_two_coupon_name,
                            salon_two_coupon_price, salon_three_coupon_name, salon_three_coupon_price,
                            first_time_money, repeat_money, salon_coupon_count, salon_menu_count, hair_style_count,
                            hair_blog_count, all_avarage_score, atmosphere_avarage, attitude_avarage, quality_avarage, price_and_menu_avarage,
                            customer_name, gender_age, posted_date, allover_review, atmosphere, attitude, quality, price_and_menu,
                            detail, selected_coupon, kind_of_menu, response]
                
                with open(save_file_path, "a", encoding='UTF-8', errors="ignore") as f:
                    writer = csv.writer(f, lineterminator='\n')
                    writer.writerow(_csv_row)                
                
                continue
            
        # サロン一覧へ戻る。(1ページ目のみ「back_url」が使えないためif-else処理。)
        if page == 1:
            driver.get(area_url)
            sleep(2)
        else:
            back_url = f"{area_url}PN{page}.html?searchGender=ALL"
            driver.get(back_url)
            sleep(2)

        # サロン一覧の次のページに進む。              
        try:
            next_button(driver=driver)
            page += 1
        # 一番最後のページの次のページはないのでエラーになる。その場合処理を終了(正常終了)
        except Exception:
            logger.info(f'{prefecture}_{area}のすべての口コミを取得(正常終了)')
            # driver.quit()でchromeを終了させる。←マルチプロセスしてる場合はやらないとダメ。
            driver.quit()
            break
        
# webdriverのオプション設定。
def webdriver_options():
    options = Options()
    # options.add_argument("--headless")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    
    driver = webdriver.Chrome("/Users/masudaniwabinari/chromedriver", options=options)
    
    return driver

def make_new_save_file(save_file_path):
    with open(save_file_path, "wt", encoding="UTF-8", errors="ignore") as f:
        csv_header = ["県", "エリア", "サロン名", "URL", "住所", "アクセス", "カット料金", "セット面の数", "ブログ投稿数", "口コミ数", "スタッフ数", "口コミ数1番人気クーポン名",
                        "1番人気クーポン価格", "2番人気クーポン名", "2番人気クーポン価格", "3番人気クーポン名", "3番人気クーポン価格","初回予約金額", "2回目以降予約金額", "クーポン数", 
                        "メニュー数", "スタイル数", "ブログ数", "口コミスコアサロン全平均", "雰囲気の全平均", "接客サービスの全平均", "技術・仕上がりの全平均", "メニュー。料金の全平均",
                        "名前", "性別、年齢、属性", "投稿日時", "総合", "雰囲気", "接客サービス", "技術・仕上がり", "メニュー・料金", "口コミ本文", "選択されたクーポン", "メニューの種類",
                        "コメントへの返信"]
        writer = csv.writer(f, lineterminator='\n')
        writer.writerow(csv_header)
        
def get_area_url(prefecture_url):
    # 例:福岡のURL → url = 'https://beauty.hotpepper.jp/svcSG/macGA/'
    driver = webdriver_options()
    driver.get(prefecture_url)
    sleep(2)
    
    webelements = driver.find_elements_by_css_selector\
        ('#mainContents > div.mT15 > div.cFix > div.searchAreaWrap > div.searchAreaListWrap > ul.searchAreaList > li')
        
    # リストの一番最初は「〜すべて」なので［:1］で省く
    area_url_list = [webelement.find_element_by_tag_name('a').get_attribute('href') for webelement in webelements[1:]]
    driver.quit()
    
    return area_url_list

#「次へ」ボタンを押す関数。(次のページに移る)
def next_button(driver):
    next_button =\
        driver.find_element_by_css_selector\
            ("div.postList.taC > ul.paging.jscPagingParents > li.pa.top0.right0.afterPage")
    next_button_url = next_button.find_element_by_tag_name("a").get_attribute("href")
    driver.get(next_button_url)
    sleep(2)
    
if __name__ == '__main__':
    prefecture_each_area_list = get_area_url(prefecture_url='https://beauty.hotpepper.jp/svcSF/macFE/')
    
    # shilkoku_all_area_list = \
    #     ['https://beauty.hotpepper.jp/svcSI/macID/salon/sacX281/',# 高知市
    #     'https://beauty.hotpepper.jp/svcSI/macIA/salon/sacX273/', 'https://beauty.hotpepper.jp/svcSI/macIA/salon/sacX274/',
    #     'https://beauty.hotpepper.jp/svcSI/macIA/salon/sacX461/',# 香川残り
    #     'https://beauty.hotpepper.jp/svcSI/macIB/salon/sacX275/', 'https://beauty.hotpepper.jp/svcSI/macIB/salon/sacX511/',#　徳島のこり
    #     'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX277/', 'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX278/',
    #     'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX579/', 'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX580/',
    #     'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX279/', 'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX280/',
    #     'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX459/', 'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX512/',
    #     'https://beauty.hotpepper.jp/svcSI/macIC/salon/sacX460/']#愛媛残り
    
    with Pool(3) as p:
        list(tqdm(p.imap_unordered(get_all_kuchikomi, prefecture_each_area_list), total=len(prefecture_each_area_list)))
    
    # get_all_kuchikomi(hukuoka_each_area_list[0])