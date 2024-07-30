from scrapy import Spider
from scrapy.http import Response


class JobDataApiComSpider(Spider):
    name = "jobdataapi_com"
    start_urls = ["https://jobdataapi.com/titles"]

    def parse(self, response):
        yield from self.extract_links(response)

    # Move logic to follow_links method for simple substitution in production
    def extract_links(self, response: Response):
        """
        Extract the href attribute from each link,
        Then yield a new request for each URL back to the parse method
        """
        target_words = ["api", "engineer", "developer", "analyst", "designer"]
        pattern = "|".join(target_words)
        # Use XPath with a regex to find all links containing any of the target words
        links = response.xpath(
            f'//a[re:test(text(), "({pattern})", "i")]/@href'
        ).getall()
        yield from response.follow_all(links, self.parse_api_response)

    def parse_api_response(self, response):
        """
        Parse the JSON response from the API
        """
        results = response.jmespath("results").getall()
        yield from results
