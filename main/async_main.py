import json
import ssl
import asyncio
from typing import List, Dict, Optional

import aiohttp
from aiohttp.connector import TCPConnector

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

# 배치 사이즈 설정
BATCH_SIZE = 5  # 동시에 처리할 최대 작업 수
REQUEST_SEMAPHORE = None  # 전역 세마포어


def create_legacy_ssl_context():
    """레거시 SSL 재협상을 허용하는 SSL 컨텍스트 생성"""
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    ctx.options |= 0x4  # OP_LEGACY_SERVER_CONNECT
    return ctx


async def custom_request(
    session: aiohttp.ClientSession, url: str, params: dict = None
) -> Optional[dict]:
    """
    주어진 URL과 파라미터로 비동기 GET 요청을 보내고 응답을 반환합니다.
    세마포어를 사용하여 동시 요청 수를 제한합니다.
    """
    async with REQUEST_SEMAPHORE:  # 세마포어로 동시 요청 수 제한
        try:
            async with session.get(
                url, params=params, timeout=aiohttp.ClientTimeout(total=10)
            ) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            print(f"요청 중 오류 발생: {e}")
            return None
        except Exception as e:
            print(f"예상치 못한 오류 발생: {e}")
            return None


async def parsing_gu_list(session: aiohttp.ClientSession, sido: str) -> List[str]:
    """
    주어진 시도에 해당하는 구 리스트를 반환합니다.
    """
    url = "https://www.hyundaicard.com/cpb/gs/apiCPBGS2005_01.hc"
    params = {
        "textCtyPrvcAddr": sido,
        "serarchType": sido,
    }
    response = await custom_request(session, url, params)

    if not response:
        return []

    # JSON 응답에서 gnguAddr만 추출
    addr_list = response.get("bdy", {}).get("result", {}).get("addrList", [])
    gu_list = [addr.get("gnguAddr", "") for addr in addr_list if addr.get("gnguAddr")]
    return gu_list


async def parsing_dong_list(
    session: aiohttp.ClientSession, sido: str, gu: str
) -> List[str]:
    """
    주어진 구에 해당하는 동 리스트를 반환합니다.
    """
    url = "https://www.hyundaicard.com/cpb/gs/apiCPBGS2005_03.hc"
    params = {
        "textCtyPrvcAddr": sido,
        "textGnguAddr": gu,
        "searchSido": sido,
        "searchSigungu": gu,
    }
    response = await custom_request(session, url, params)

    if not response:
        return []

    # JSON 응답에서 vlgAddr만 추출
    addr_list = response.get("bdy", {}).get("result", {}).get("addrList", [])
    do_list = [addr.get("vlgAddr", "") for addr in addr_list if addr.get("vlgAddr")]
    return do_list


async def parsing_dong_store(
    session: aiohttp.ClientSession, sido: str, gu: str, dong: str
) -> List[Dict[str, str]]:
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
        response = await custom_request(session, url, params)

        if not response:
            break

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


async def process_batch(tasks: List, batch_size: int = BATCH_SIZE):
    """
    태스크를 배치 단위로 처리합니다.
    """
    results = []
    for i in range(0, len(tasks), batch_size):
        batch = tasks[i : i + batch_size]
        print(f"배치 {i // batch_size + 1} 처리 중... ({len(batch)}개 작업)")

        batch_results = await asyncio.gather(*batch, return_exceptions=True)

        # 예외 처리
        for result in batch_results:
            if isinstance(result, Exception):
                print(f"배치 처리 중 오류 발생: {result}")
                results.append([])  # 빈 리스트로 대체
            else:
                results.append(result)

        # 배치 간 잠시 대기 (서버 부하 방지)
        if i + batch_size < len(tasks):
            await asyncio.sleep(0.1)

    return results


async def process_sido(
    session: aiohttp.ClientSession, sido: str
) -> List[Dict[str, str]]:
    """시도별 매장 정보를 처리합니다."""
    print(f"Parsing {sido}...")
    gu_list = await parsing_gu_list(session, sido)

    if not gu_list:
        print(f"{sido}: 구 정보가 없습니다.")
        return []

    # 구별 처리 태스크 생성
    gu_tasks = []
    for gu in gu_list:
        gu_tasks.append(process_gu(session, sido, gu))

    # 배치 단위로 처리
    gu_results = await process_batch(gu_tasks)

    # 결과 합치기
    sido_stores = []
    for stores in gu_results:
        if stores:  # None이나 빈 리스트가 아닌 경우
            sido_stores.extend(stores)

    print(f"Finished parsing {sido}. (총 {len(sido_stores)}개 매장)\n")
    return sido_stores


async def process_gu(
    session: aiohttp.ClientSession, sido: str, gu: str
) -> List[Dict[str, str]]:
    """구별 매장 정보를 처리합니다."""
    print(f"  Parsing {sido} - {gu}...")
    dong_list = await parsing_dong_list(session, sido, gu)

    if not dong_list:
        print(f"    {sido} - {gu}: 동 정보가 없습니다.")
        return []

    print(f"    {sido} - {gu}: {len(dong_list)}개 동 발견")

    # 동별 처리 태스크 생성
    dong_tasks = []
    for dong in dong_list:
        dong_tasks.append(process_dong(session, sido, gu, dong))

    # 배치 단위로 처리 (동 단위는 작은 배치 사이즈 사용)
    dong_results = await process_batch(dong_tasks, batch_size=3)

    # 결과 합치기
    gu_stores = []
    for stores in dong_results:
        if stores:  # None이나 빈 리스트가 아닌 경우
            gu_stores.extend(stores)

    print(f"    {sido} - {gu} 완료. ({len(gu_stores)}개 매장)")
    return gu_stores


async def process_dong(
    session: aiohttp.ClientSession, sido: str, gu: str, dong: str
) -> List[Dict[str, str]]:
    """동별 매장 정보를 처리합니다."""
    store_list = await parsing_dong_store(session, sido, gu, dong)
    if store_list:
        print(f"      {sido} - {gu} - {dong}: {len(store_list)}개 매장")
    return store_list


async def main(batch_size: int = BATCH_SIZE, max_concurrent_requests: int = 10):
    """
    메인 비동기 함수

    Args:
        batch_size: 배치 처리 단위 (기본값: 5)
        max_concurrent_requests: 최대 동시 요청 수 (기본값: 10)
    """
    global REQUEST_SEMAPHORE, BATCH_SIZE

    BATCH_SIZE = batch_size
    REQUEST_SEMAPHORE = asyncio.Semaphore(max_concurrent_requests)

    print(f"배치 사이즈: {batch_size}, 최대 동시 요청 수: {max_concurrent_requests}")
    print("=" * 50)

    # SSL 컨텍스트와 커넥터 설정
    ssl_context = create_legacy_ssl_context()
    connector = TCPConnector(ssl=ssl_context, limit=max_concurrent_requests * 2)

    async with aiohttp.ClientSession(connector=connector) as session:
        # 시도별 태스크 생성
        tasks = []
        for sido in SIDO_LIST:
            tasks.append(process_sido(session, sido))

        # 배치 단위로 시도 처리
        results = await process_batch(tasks, batch_size=batch_size)

        # 결과 합치기
        nationwide_store_list = []
        for stores in results:
            if stores:  # None이나 빈 리스트가 아닌 경우
                nationwide_store_list.extend(stores)

        print("=" * 50)
        print(f"전체 처리 완료! 총 매장 수: {len(nationwide_store_list)}")
        return nationwide_store_list


if __name__ == "__main__":
    # 설정 가능한 매개변수
    BATCH_SIZE_CONFIG = 3  # 동시에 처리할 시도/구/동 수
    MAX_REQUESTS_CONFIG = 8  # 최대 동시 HTTP 요청 수

    # 비동기 실행
    nationwide_stores = asyncio.run(
        main(batch_size=BATCH_SIZE_CONFIG, max_concurrent_requests=MAX_REQUESTS_CONFIG)
    )

    # 결과 출력 (처음 5개만)
    print("\n처음 5개 매장 정보:")
    for i, store in enumerate(nationwide_stores[:5]):
        print(f"{i + 1}. {store}")

    print(f"총 {len(nationwide_stores)}개의 매장을 수집했습니다.")

    # JSON 파일로 저장
    with open("../nationwide_stores.json", "w", encoding="utf-8") as f:
        json.dump(nationwide_stores, f, ensure_ascii=False, indent=4)

    print("nationwide_stores.json 파일로 저장 완료!")

    # 콘솔에도 출력 (선택사항)
    print(json.dumps(nationwide_stores, ensure_ascii=False, indent=4))
