from itemadapter import ItemAdapter
from zyte_common_items import ZyteItemAdapter

ItemAdapter.ADAPTER_CLASSES.appendleft(ZyteItemAdapter)

BOT_NAME = "whoshiring_aggregator"

SPIDER_MODULES = [
    "zyte_spider_templates.spiders",
    "whoshiring_aggregator.spiders",
]
NEWSPIDER_MODULE = "whoshiring_aggregator.spiders"

CLOSESPIDER_TIMEOUT_NO_ITEM = 900
ADDONS = {
    "scrapy_zyte_api.Addon": 500,
    "duplicate_url_discarder.Addon": 600,
}
SCHEDULER_DISK_QUEUE = "scrapy.squeues.PickleFifoDiskQueue"
SCHEDULER_MEMORY_QUEUE = "scrapy.squeues.FifoMemoryQueue"
SPIDER_MIDDLEWARES = {
    "scrapy_poet.RetryMiddleware": 275,
    "scrapy.spidermiddlewares.offsite.OffsiteMiddleware": None,
    "zyte_spider_templates.middlewares.AllowOffsiteMiddleware": 500,
    "zyte_spider_templates.middlewares.CrawlingLogsMiddleware": 1000,
}

# scrapy-poet
SCRAPY_POET_DISCOVER = [
    "zyte_spider_templates.pages",
    "whoshiring_aggregator.pages",
]

# duplicate-url-discarder
DUD_ATTRIBUTES_PER_ITEM = {
    "zyte_common_items.Product": [
        "canonicalUrl",
        "brand",
        "name",
        "gtin",
        "mpn",
        "productId",
        "sku",
        "color",
        "size",
        "style"
    ],
}
