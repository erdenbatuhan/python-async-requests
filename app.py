import sys
import itertools

import pandas as pd
import aiohttp
import asyncio

# Concurrency constants
NUM_OF_CONCURRENT_SESSIONS = 7

# Messari constants
MESSARI_ASSETS_API_GET_ALL_V2 = "https://data.messari.io/api/v2/assets" \
                                "?page=%d" \
                                "&limit=500" \
                                "&fields=id,slug,symbol,name,metrics/market_data/price_usd,metrics/marketcap/rank"
MESSARI_ASSETS_INDEX_ATTRIBUTE = "id"

# CSV constants
CSV_DEFAULT_FILENAME = "cryptocurrencies.csv"
CSV_COLUMNS = ["ID", "Symbol", "Name", "Slug", "Price in USD", "Rank"]
CSV_INDEX_COLUMN = "ID"


async def get_one_page_asset_data_from_messari(session, endpoint):
    """
    Gets a one-page-long asset data from Messari API

    :param session: aiohttp Client Session
    :param endpoint: endpoint to call for one page of data
    :return: asset data for that one page, empty if the page limit has been reached
    """
    async with session.get(endpoint) as resp:
        resp = await resp.json()

        # check if the error was expected (404 = the page limit has been reached!)
        if "error_code" in resp["status"]:
            if resp["status"]["error_code"] == 404:
                return []

            raise Exception(resp["status"]["error_message"])

        return resp["data"]


async def get_assets_from_messari():
    """
    Gets multiple pages of asset data from Messari API.
    Note: Sometimes, the retrieval won't succeed. In that case, we try again for a few times.

    :return: asset data for all of the pages
    """
    print("Getting all of the assets from Messari API in parallel..")
    all_asset_data = []

    async with aiohttp.ClientSession() as session:
        step = NUM_OF_CONCURRENT_SESSIONS

        # fetch pages in parallel until there are none left
        # step is the number of pages fetched simultaneously
        for i in itertools.count(start=1, step=step):
            tasks = [
                asyncio.ensure_future(get_one_page_asset_data_from_messari(
                    session=session, endpoint=MESSARI_ASSETS_API_GET_ALL_V2 % page
                ))
                for page in range(i, i + step)
            ]

            asset_data_gathered = await asyncio.gather(*tasks)

            # append the pages fetched to the final asset data
            for item in asset_data_gathered:
                all_asset_data += item

            # if the response for a single request is an empty array, there are no new pages left to fetch!
            if any(len(item) == 0 for item in asset_data_gathered):
                break

    print("The assets have successfully been retrieved!", end="\n\n")
    return all_asset_data


def read_cryptocurrencies_from_csv(filename=CSV_DEFAULT_FILENAME):
    """
    Reads existing cryptocurrencies from the csv file.

    :param filename: name of the csv file the data is read from (default: CSV_DEFAULT_FILENAME)
    :return: cryptocurrency data read from the csv file
    """
    try:
        crypto_df = pd.read_csv(filename)
    except FileNotFoundError:
        crypto_df = pd.DataFrame([], columns=CSV_COLUMNS)

    # set the index for faster access
    crypto_df.set_index(CSV_INDEX_COLUMN, inplace=True)

    return crypto_df


def update_crypto_dict_with_messari_asset(crypto_dict, order, asset):
    """
    Updates the given cryptocurrency dictionary with the asset data fetched from Messari

    :param crypto_dict: the cryptocurrency dictionary updated
    :param order: order of the asset in the Messari API response
    :param asset: one asset in the Messari API response
    :return: None (the dictionary given is updated!)
    """
    index = asset[MESSARI_ASSETS_INDEX_ATTRIBUTE]

    crypto_dict["Order"][index] = order
    crypto_dict["Symbol"][index] = asset["symbol"]
    crypto_dict["Name"][index] = asset["name"]
    crypto_dict["Slug"][index] = asset["slug"]
    crypto_dict["Price in USD"][index] = asset["metrics"]["market_data"]["price_usd"]
    crypto_dict["Rank"][index] = asset["metrics"]["marketcap"]["rank"]


def upsert_cryptocurrencies(crypto_df, updated_assets):
    """
    Inserts new cryptocurrencies and updates existing ones.
    Making use of the key/value structure of dictionaries reduces the complexity.

    :param crypto_df: cryptocurrency data read from the csv file
    :param updated_assets: cryptocurrency data read from the Messari API
    :return: updated cryptocurrency data
    """
    # convert the DataFrame to a dictionary
    crypto_dict = crypto_df.to_dict()

    # store the ordering
    crypto_dict["Order"] = {}

    # add a new cryptocurrency or update an existing one
    for order, asset in enumerate(updated_assets):
        update_crypto_dict_with_messari_asset(crypto_dict, order, asset)

    # return the dictionary as a DataFrame
    return pd.DataFrame.from_dict(crypto_dict)


def sort_cryptocurrencies(crypto_df):
    """
    Sorts the cryptocurrencies in the same order as the API response.
    The Order column is filled in upsert_cryptocurrencies() function.

    :param crypto_df: cryptocurrency data that is to be sorted
    :return: None (the data given is updated!)
    """
    crypto_df.sort_values(by=["Order"], inplace=True)
    crypto_df.drop(columns=["Order"], inplace=True)


def save_cryptocurrencies_to_csv(crypto_df, filename=CSV_DEFAULT_FILENAME):
    """
    Saves the new and updated cryptocurrencies to the csv file.

    :param crypto_df: final cryptocurrency data that is to be written
    :param filename: name of the csv file the data is written to (default: CSV_DEFAULT_FILENAME)
    :return: None
    """
    crypto_df.to_csv(filename, index=True, index_label=CSV_INDEX_COLUMN)


if __name__ == "__main__":
    # read filename from arguments if it's given
    if len(sys.argv) == 2:
        csv_filename = str(sys.argv[1])
    else:
        csv_filename = CSV_DEFAULT_FILENAME

    # get the response in parallel
    event_loop = asyncio.get_event_loop()
    assets = event_loop.run_until_complete(get_assets_from_messari())

    # CSV: insert/update
    df = read_cryptocurrencies_from_csv(filename=csv_filename)
    df = upsert_cryptocurrencies(crypto_df=df, updated_assets=assets)

    # CSV: sort and save
    sort_cryptocurrencies(crypto_df=df)
    save_cryptocurrencies_to_csv(crypto_df=df, filename=csv_filename)

    print(df)
