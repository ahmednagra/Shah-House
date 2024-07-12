from .autotrader1 import AutotraderSpider


class AutotraderSpider2(AutotraderSpider):
    name = 'autotrader2'

    def parse(self, response, **kwargs):
        # makers = response.css('[label="All Makes"] option')
        makers = response.css('[label="All Makes"] option')

        makers = [maker.css('::attr(value)').get('') for maker in makers if maker.css('::attr(label)').get('').lower() not in self.make_filter]

        self.makers = makers[35:]
