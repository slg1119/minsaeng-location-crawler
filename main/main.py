import json
import ssl

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import create_urllib3_context

SIDO_LIST = [
    "서울",
    "부산",
    "대구",
    "인천",
    "광주",
    "대전",
    "울산",
    "경기",
    "강원",
    "충북",
    "충남",
    "전북",
    "전남",
    "경북",
    "경남",
    "제주",
    "세종",
]


class CustomHTTPSAdapter(HTTPAdapter):
    """SSL 레거시 재협상을 허용하는 커스텀 어댑터"""

    def __init__(self, ssl_context=None, **kwargs):
        self.ssl_context = ssl_context
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs["ssl_context"] = self.ssl_context
        return super().init_poolmanager(*args, **kwargs)


def create_legacy_ssl_context():
    """레거시 SSL 재협상을 허용하는 SSL 컨텍스트 생성"""
    ctx = create_urllib3_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    return ctx


def custom_request(url: str, params: dict = None) -> requests.Response:
    """
    주어진 URL과 파라미터로 GET 요청을 보내고 응답을 반환합니다.
    """
    session = requests.Session()
    session.mount("https://", CustomHTTPSAdapter(create_legacy_ssl_context()))

    try:
        response = session.get(url, params=params, verify=False, timeout=10)
        response.raise_for_status()
        return response.json()  # JSON 응답을 반환합니다.
    except requests.exceptions.RequestException as e:
        print(f"요청 중 오류 발생: {e}")
        return None
    except Exception as e:
        print(f"예상치 못한 오류 발생: {e}")
        return None


def parsing_gu_list(sido: str) -> list:
    """
    주어진 시도에 해당하는 구 리스트를 반환합니다.
    """
    url = "https://www.hyundaicard.com/cpb/gs/apiCPBGS2005_01.hc"
    params = {
        "textCtyPrvcAddr": sido,
        "serarchType": sido,
    }
    response = custom_request(url, params)

    # JSON 응답에서 gnguAddr만 추출
    addr_list = response.get("bdy", {}).get("result", {}).get("addrList", [])
    gu_list = [addr.get("gnguAddr", "") for addr in addr_list if addr.get("gnguAddr")]
    return gu_list


def parsing_dong_list(sido: str, gu: str) -> list:
    """
    주어진 구에 해당하는 도 리스트를 반환합니다.
    """
    url = "https://www.hyundaicard.com/cpb/gs/apiCPBGS2005_03.hc"
    params = {
        "textCtyPrvcAddr": sido,
        "textGnguAddr": gu,
        "searchSido": sido,
        "searchSigungu": gu,
    }
    response = custom_request(url, params)

    # JSON 응답에서 doAddr만 추출
    addr_list = response.get("bdy", {}).get("result", {}).get("addrList", [])
    do_list = [addr.get("vlgAddr", "") for addr in addr_list if addr.get("vlgAddr")]
    return do_list


def parsing_dong_store(sido: str, gu: str, dong: str) -> list:
    """
    주어진 구와 동에 해당하는 매장 리스트를 반환합니다.
    """
    next_cond = 0
    next_key = 1
    url = "https://www.hyundaicard.com/cpb/gs/apiCPBGS2005_04.hc"
    all_stores = []

    while True:
        params = {
            "textCtyPrvcAddr": sido,
            "textGnguAddr": gu,
            "textVlgAddr": dong,
            "nextCond": str(next_cond),
            "nextkey": str(next_key),
            "searchSido": sido,
            "searchSigungu": gu,
            "searchDong": dong,
        }
        response = custom_request(url, params)

        # JSON 응답에서 매장 정보 추출
        ha38271003TO = response.get("bdy", {}).get("result", {}).get("ha38271003TO", {})
        store_list = ha38271003TO.get("gridpgt1", [])

        if not store_list:
            break

        # 각 매장의 필요한 정보만 추출
        for store in store_list:
            store_info = {
                "store_name": store.get("mrchConmNm", ""),  # 상호명
                "address": store.get("mrchAdst", ""),  # 주소
                "phone": store.get("mrchTno", ""),  # 전화번호
                "sido": sido,  # 시도
                "gu": gu,  # 구
                "dong": dong,  # 동
            }
            all_stores.append(store_info)

        next_cond += 1
        next_key += 1

    return all_stores


if __name__ == "__main__":
    nationwide_store_list = []
    for sido in SIDO_LIST:
        print(f"Parsing {sido}...")
        gu_list: list = parsing_gu_list(sido)

        for gu in gu_list:
            print(f"Parsing {sido} - {gu}...")
            dong_list: list = parsing_dong_list(sido, gu)
            print(dong_list)
            for dong in dong_list:
                print(f"Parsing {sido} - {gu} - {dong}...")
                store_list: list = parsing_dong_store(sido, gu, dong)
                nationwide_store_list.extend(store_list)
                print(store_list)
        print(f"Finished parsing {sido}.\n")

    print(f"총 {len(nationwide_store_list)}개의 매장을 수집했습니다.")

    # JSON 파일로 저장
    with open("nationwide_stores.json", "w", encoding="utf-8") as f:
        json.dump(nationwide_store_list, f, ensure_ascii=False, indent=4)

    print("nationwide_stores.json 파일로 저장 완료!")

    # 콘솔에도 출력 (선택사항)
    print(json.dumps(nationwide_store_list, ensure_ascii=False, indent=4))
