import requests
from pymongo import MongoClient

MONGODB_URL = 'mongodb://root:example@localhost:27017/'
PAGE_SIZE = 50
LULU_DOMAIN = 'https://shop.lululemon.com'

def main():
    db = DB()
    lulu_graphql_url = "https://shop.lululemon.com/snb/graphql"
    request_hearders = {"Content-Type": "application/json", "accept": "application/json"}
    query_str = {
      "query": "query CategoryPageDataQuery($category:String!,$cid:String,$forceMemberCheck:Boolean,$nValue:String!,$sl:String!,$locale:String!,$Ns:String,$storeId:String,$pageSize:Int,$page:Int,$onlyStore:Boolean,$useHighlights:Boolean,$abFlags:[String]){categoryPageData(category:$category nValue:$nValue locale:$locale sl:$sl Ns:$Ns page:$page pageSize:$pageSize storeId:$storeId onlyStore:$onlyStore forceMemberCheck:$forceMemberCheck cid:$cid useHighlights:$useHighlights abFlags:$abFlags){activeCategory allLocaleNvalues{CA US __typename}categoryLabel fusionExperimentId fusionExperimentVariant fusionQueryId h1Title isBopisEnabled isFusionQuery isWMTM name results:totalProducts totalProductPages currentPage type redirectResponse{url __typename}clearAction{label navigationState siteRootPath contentPath link __typename}breadcrumbs{label navigationState contentPath __typename}sortOptions{contentPath text:label value:link navigationState selected className __typename}facet{configurableNavigation{name type linkText url __typename}dimensionName enableVisualColorStyling enableVisualSizeStyling multiSelect name noFollow noFollowBool primaryNumRefinements refinements{count colorGroup contentPath label description trademarkLabel navigationState removeAction{navigationState __typename}isSelected:selected sizeGroup __typename}selectedCount showMoreRefinementsLink type __typename}refinementCrumbs{ancestors{label properties{unifiedId __typename}__typename}categoryLabel count dimensionName displayName label properties{canonicalUrl headData longDescription noFollow noIndex title __typename}removeAction{navigationState contentPath __typename}__typename}products{allAvailableSizes currencyCode defaultSku displayName intendedCupSize listPrice parentCategoryUnifiedId productOnSale:onSale productSalePrice:salePrice pdpUrl productCoverage repositoryId:productId productId inStore unifiedId highlights{highlightLabel highlightIconWeb __typename}skuStyleOrder{colorGroup colorId colorName inStore size sku skuStyleOrderId styleId01 styleId02 styleId __typename}swatches{primaryImage hoverImage url colorId inStore __typename}__typename}bopisProducts{allAvailableSizes currencyCode defaultSku displayName listPrice parentCategoryUnifiedId productOnSale:onSale productSalePrice:salePrice pdpUrl productCoverage repositoryId:productId productId inStore unifiedId highlights{highlightLabel highlightIconWeb __typename}skuStyleOrder{colorGroup colorId colorName inStore size sku skuStyleOrderId styleId01 styleId02 styleId __typename}swatches{primaryImage hoverImage url colorId inStore __typename}__typename}storeInfo{totalInStoreProducts totalInStoreProductPages storeId __typename}seoLinks{next prev self __typename}__typename}}",
      "operationName": "CategoryPageDataQuery",
      "variables": {
        "nValue": "N-8t6",
        "category": "sale",
        "locale": "en_US",
        "sl": "US",
        "page": 1,
        "pageSize": PAGE_SIZE,
        "forceMemberCheck": False,
        "abFlags": [],
        "Ns": "product.last_SKU_addition_dateTime|1"
      }
    }

    current_count = 0
    # parse all data in a loop
    while True:
        # query from lululemon graphql
        res = requests.post(lulu_graphql_url, json=query_str, headers=request_hearders)
        products = res.json()['data']['categoryPageData']['products']
        current_count += len(products)
        query_str["variables"]["page"] += 1
        total_count = res.json()['data']['categoryPageData']['results']
        print(f"{current_count}/{total_count}")

        # Persist to mongodb
        db.save_lulu_prods(products)

        # Check if all data has been parsed
        if current_count >= total_count:
            break


def convert_price(price) -> str:
    if len(price) == 1:
        return f"${int(float(price[0]))}"
    elif len(price) >= 2:
        return f"${int(float(price[0]))} - ${int(float(price[1]))}"


def convert_product(product):
    res = {
        'title': product['displayName'],
        'buylink': LULU_DOMAIN + product['pdpUrl'],
        'price': convert_price(product['productSalePrice']),
        'old_price': convert_price(product['listPrice']),
        'image_url': product['swatches'][0]['primaryImage'],
    }
    return res


class DB:
    def __init__(self):
        self.client = MongoClient(MONGODB_URL)
        self.db = self.client['crawler']
        self.col = self.db['lulu']
        self.idx = 1

    def save_lulu_prods(self, q):
        res = list(map(convert_product, q))
        for i in range(len(res)):
            res[i]['No.'] = self.idx
            self.idx += 1

        self.col.insert_many(res)


if __name__ == "__main__":
    main()
