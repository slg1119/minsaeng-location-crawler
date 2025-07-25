import json
import requests


def parsing_sido() -> list:
    """시도 목록을 파싱합니다."""
    url = "https://www.shinhancard.com/mob/MOBFM591N/MOBFM591R0301.ajax?mbw_json=%7B%22qyVl%22%3A%22sido%22%7D"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch sido data: {response.status_code}")

    data = response.json()
    return data.get("mbw_json").get("cityList", [])


def parsing_sigungu(widCtyCd: int) -> list:
    """시군구 목록을 파싱합니다."""
    url = f"https://www.shinhancard.com/mob/MOBFM591N/MOBFM591R0301.ajax?mbw_json=%7B%22qyVl%22%3A%22sigungu%22%2C%22sido%22%3A%22{widCtyCd}%22%7D"
    response = requests.get(url)

    if response.status_code != 200:
        raise Exception(f"Failed to fetch sigungu data: {response.status_code}")
    data = response.json()
    return data.get("mbw_json").get("sigunguList", [])


def parsing_store(widCtyCd: int, gdsCtyCd: int, gdsBrgCd: int) -> list:
    """매장 목록을 파싱합니다."""
    url = f"https://www.shinhancard.com/mob/MOBFM591N/MOBFM591R0304.ajax?mbw_json=%7B%22lastMctNm%22%3A%22%22%2C%22lastMctN%22%3A%22%22%2C%22locationNm%22%3A%22%22%2C%22widCtyCd%22%3A%22{widCtyCd}%22%2C%22gdsCtyCd%22%3A%22{gdsCtyCd}%22%2C%22hpsnClnAbnCd%22%3A%22{widCtyCd}{gdsCtyCd}{gdsBrgCd}%22%2C%22hpsnMctNm%22%3A%22%22%2C%22hpsnSciMctRyZcd%22%3A%2206%22%7D"
    response = requests.get(url)
    if response.status_code != 200:
        raise Exception(f"Failed to fetch store data: {response.status_code}")
    data = response.json()
    return [
        {
            "name": store["mctNm"],
            "lng": store["lalMctXcVl"],
            "lat": store["lalMctYcVl"],
            "category": store["hpsnMctZcdNm"],
            "phone": store["mctPon"],
            "address": store["gdsAfMctAr"],
        }
        for store in data["mbw_json"]["list"]
    ]


store_list = []
sido_list = parsing_sido()
for sido in sido_list:
    print(f"시도: {sido['gdsWidTrlNm']} (코드: {sido['widCtyCd']})")
    sigungu_list = parsing_sigungu(sido["widCtyCd"])
    for sigungu in sigungu_list:
        print(
            f"  시군구: {sigungu['gdsCtyBrgCdNm']} (코드: {sigungu['gdsCtyCd']}) (코드 2: {sigungu['gdsBrgCd']})"
        )
        store_list.extend(
            parsing_store(sido["widCtyCd"], sigungu["gdsCtyCd"], sigungu["gdsBrgCd"])
        )

print(f"총 {len(store_list)}개의 매장 정보가 수집되었습니다.")

with open("store_list.json", "w") as f:
    json.dump(store_list, f, ensure_ascii=False, indent=4)
