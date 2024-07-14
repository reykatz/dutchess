import json
import aiolimiter
from box import Box
from loguru import logger
import requests
import asyncio
import itertools

logger.disable(__name__)
rate_limiter = aiolimiter.AsyncLimiter(10, 1)

async def query(*args, **kwargs):
    async with rate_limiter:
        return await raw_query(*args, **kwargs)

async def raw_query(session, op_name, query_hash, variables):
    extensions = dict(persistedQuery=dict(
        version=1, sha256Hash=query_hash))
    params = dict(
        operationName=op_name,
        variables=json.dumps(variables),
        extensions=json.dumps(extensions))

    request = requests.Request("GET", "https://dutchie.com/graphql", params=params).prepare()
    # I used the following two lines to test the working URL copied directly from the site, with an additional request header. The original code started working after that. It may be because I copied pasted the working hash one more time. -Rey
    # request = requests.Request("GET", "https://dutchie.com/graphql?operationName=ConsumerDispensaries&variables=%7B%22dispensaryFilter%22%3A%7B%22cNameOrID%22%3A%22liberty-somerville%22%7D%7D&extensions=%7B%22persistedQuery%22%3A%7B%22version%22%3A1%2C%22sha256Hash%22%3A%22389b0510b406db377723755723e0ed1b545f295a8b440c5525928224a3edc486%22%7D%7D").prepare()
    # request.headers["X-Dutchie-Session"] = "session=eyJpZCI6IjZmZDcyNzE1LWM4MmItNDMzYS1iMThkLTk0NzIyMDc0YzhjNyIsImV4cGlyZXMiOjE3MTIzMzk0OTg3NDJ9;session_sig=6T3fo22obkD_8N2vDvH59kqy_io"
    logger.debug("Fetching from upstream.", query_params=params, url=request.url)
    request.headers["Content-Type"] = "application/json"
    response = await asyncio.to_thread(session.send, request)
    logger.debug("Response", elapsed=response.elapsed.total_seconds(), size=len(response.content))
    try:
        response.raise_for_status()
    except Exception as e:
        e.add_note(response.content.decode('utf-8'))
        raise
    raw_data = response.json()
    if 'data' not in raw_data:
        raise ValueError(raw_data)
    data = raw_data.pop('data')
    if raw_data:
        logger.debug("Unconsumed response fields. ", raw_data=raw_data)
    return data

async def dispensary_query(session, distance):
    variables = dict(
        dispensaryFilter=dict(
            medical=True,
            recreational=False,
            sortBy="distance",
            activeOnly=True,
            city="Somerville",
            country = "United States",
            nearLat = 42.387597,
            nearLng = -71.099497,
            destinationTaxState = "MA",
            distance = distance,
            openNowForPickup = False,
            acceptsCreditCardsPickup = False,
            acceptsDutchiePay = False,
            offerCurbsidePickup = False,
            offerPickup = True))

    # TO GET NEW HASH: go to URL: https://dutchie.com/dispensary/Ethos-Watertown-Medical/product/gelatti-cookies
    # Open Network requests, filter for XHR or "graphql" and find consumer dispensaries request
    # In request URL, copy hash value (between %22 characters or spaces)
    # copy paste hash into here in a new line
    data = await query(session,
                       "ConsumerDispensaries",
                       "10f05353272bab0f3ceeff818feef0a05745241316be3a5eb9f3e66ca927a40d",
                       variables)
    return [Box(d) for d in data['filteredDispensaries']]


async def menu_query(session, dispensary_id):
    variables = dict(
        includeEnterpriseSpecials = False,
        includeTerpenes = True,
        includeCannabinoids = True,
        productsFilter = dict(
            dispensaryId = dispensary_id,
            pricingType = "med",
            Status = "Active",
            strainTypes = [],
            types = ["Flower"],
            bypassOnlineThresholds = False,
            isKioskMenu = False,
            removeProductsBelowOptionThresholds = True))

    data = await query(session,
                       "IndividualFilteredProduct",
                       "63df54bf308c5176e422805961f73ebdda75ae3a60f9885831be146b4a7cb32e",
                       variables)

    return [Box(p) for p in data['filteredProducts']['products']]

async def load(distance=25):
    with requests.Session() as session:
        ds = await dispensary_query(session, distance)
        menus = await asyncio.gather(*[menu_query(session, d.id) for d in ds])
        ps = [p for p in itertools.chain.from_iterable(menus)]
        return ds, ps
