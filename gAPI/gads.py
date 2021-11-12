import pandas as pd
from basic.date import get_today, datetime_to_str
from google.ads.googleads.client import GoogleAdsClient

# Location IDs are listed here:
# https://developers.google.com/google-ads/api/reference/data/geotargets
# and they can also be retrieved using the GeoTargetConstantService as shown
# here: https://developers.google.com/google-ads/api/docs/targeting/location-targeting
_DEFAULT_LOCATION_IDS = ["1023191"]  # location ID for New York, NY
# A language criterion ID. For example, specify 1000 for English. For more
# information on determining this value, see the below link:
# https://developers.google.com/google-ads/api/reference/data/codes-formats#expandable-7
_DEFAULT_LANGUAGE_ID = "1000"  # language ID for English

class GoogleAds:
    def __init__(self, path_ads_config='google-ads.yaml', location_ids=['9075967'], language_id='1018', customer_id='6553682300'):
        self.location_ids = location_ids  ## Taiwan: 9075967 (TW)
        self.language_id = language_id  ## zh_TW
        self.customer_id = customer_id
        self.client = GoogleAdsClient.load_from_storage(version="v8", path=path_ads_config)
        self.month_mapping = {'JANUARY':'01', 'FEBRUARY':'02', 'MARCH':'03', 'APRIL':'04', 'MAY':'05', 'JUNE':'06',
                              'JULY':'07', 'AUGUST':'08', 'SEPTEMBER':'09', 'OCTOBER':'10', 'NOVEMBER':'11', 'DECEMBER':'12'}
        self.keyword_competition_levels = ['HIGH', 'MEDIUM', 'LOW', 'UNKNOWN', 'UNSPECIFIED']

    def get_keyword_cpc(self, keyword):
        client = self.client
        location_ids = self.location_ids
        language_id = self.language_id
        customer_id = self.customer_id
        if type(keyword) == str:
            keyword = [keyword]
        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        keyword_competition_level_enum = (client.enums.KeywordPlanCompetitionLevelEnum)
        keyword_plan_network = (client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS)
        location_rns = self._map_locations_ids_to_resource_names(client, location_ids)
        language_rn = client.get_service("LanguageConstantService").language_constant_path(language_id)
        # Only one of the fields "url_seed", "keyword_seed", or
        # "keyword_and_url_seed" can be set on the request, depending on whether
        # keywords, a page_url or both were passed to this function.
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language = language_rn
        request.geo_target_constants = location_rns
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network
        request.keyword_seed.keywords.extend(keyword)

        keyword_idea = keyword_plan_idea_service.generate_keyword_ideas(
            request=request
        ).results[0]
        low_price = keyword_idea.keyword_idea_metrics.low_top_of_page_bid_micros / 1000000
        high_price = keyword_idea.keyword_idea_metrics.high_top_of_page_bid_micros / 1000000
        return low_price, high_price

    def get_keyword_list_info(self, keyword_list):
        client = self.client
        location_ids = self.location_ids
        language_id = self.language_id
        customer_id = self.customer_id

        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        keyword_competition_level_enum = (client.enums.KeywordPlanCompetitionLevelEnum)
        keyword_plan_network = (client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS)
        location_rns = self._map_locations_ids_to_resource_names(client, location_ids)
        language_rn = client.get_service("LanguageConstantService").language_constant_path(language_id)
        # Only one of the fields "url_seed", "keyword_seed", or
        # "keyword_and_url_seed" can be set on the request, depending on whether
        # keywords, a page_url or both were passed to this function.
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language = language_rn
        request.geo_target_constants = location_rns
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network
        request.keyword_seed.keywords.extend(keyword_list)

        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(request=request)
        keywords_info = {}
        i = 0
        date_update = datetime_to_str(get_today())
        keyword_list_noblank = [keyword.replace(' ','') for keyword in keyword_list]
        # month_desired = get_today().month-1
        # year_desired = get_today().year
        # print(keyword_list_noblank)
        for idea in keyword_ideas:
            # print(idea.text.replace(' ',''))
            idea_join = idea.text.replace(' ','')
            if idea_join in keyword_list_noblank:
                index = keyword_list_noblank.index(idea_join)
                competition_level = idea.keyword_idea_metrics.competition.name
                competition_value = idea.keyword_idea_metrics.competition.value
                low_price = idea.keyword_idea_metrics.low_top_of_page_bid_micros / 1000000
                high_price = idea.keyword_idea_metrics.high_top_of_page_bid_micros / 1000000
                avg_monthly_traffic = idea.keyword_idea_metrics.avg_monthly_searches
                keywords_info[i] = {'keyword_ask': keyword_list[index], 'keyword_join': idea_join,
                                    'keyword_google': idea.text,
                                    'competition_level': competition_level, 'competition_value': competition_value,
                                    'low_price': low_price, 'high_price': high_price,
                                    'avg_monthly_traffic': avg_monthly_traffic, 'date': date_update}
                i += 1
        df = pd.DataFrame.from_dict(keywords_info, "index")
        return df

    ## including its related queries
    def get_keyword_info(self, keyword):
        client = self.client
        location_ids = self.location_ids
        language_id = self.language_id
        customer_id = self.customer_id
        if type(keyword) == str:
            keyword = [keyword]
        keyword_plan_idea_service = client.get_service("KeywordPlanIdeaService")
        keyword_competition_level_enum = (client.enums.KeywordPlanCompetitionLevelEnum)
        keyword_plan_network = (client.enums.KeywordPlanNetworkEnum.GOOGLE_SEARCH_AND_PARTNERS)
        location_rns = self._map_locations_ids_to_resource_names(client, location_ids)
        language_rn = client.get_service("LanguageConstantService").language_constant_path(language_id)

        # Only one of the fields "url_seed", "keyword_seed", or
        # "keyword_and_url_seed" can be set on the request, depending on whether
        # keywords, a page_url or both were passed to this function.
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = customer_id
        request.language = language_rn
        request.geo_target_constants = location_rns
        request.include_adult_keywords = False
        request.keyword_plan_network = keyword_plan_network
        request.keyword_seed.keywords.extend(keyword)

        keyword_ideas = keyword_plan_idea_service.generate_keyword_ideas(request=request)
        keywords_info = {}
        # keyword_main = keyword_ideas[0].text
        i = 0
        date_update = datetime_to_str(get_today())
        for idea in keyword_ideas:
            competition_value = idea.keyword_idea_metrics.competition.name
            low_price = idea.keyword_idea_metrics.low_top_of_page_bid_micros / 1000000
            high_price = idea.keyword_idea_metrics.high_top_of_page_bid_micros / 1000000
            searchs_month = idea.keyword_idea_metrics.monthly_search_volumes ## list
            avg_monthly_traffic = idea.keyword_idea_metrics.avg_monthly_searches
            for search in searchs_month:
                year, month, traffic = search._pb.year, search._pb.month-1, search._pb.monthly_searches
                date = f"{year}-{month:02}-01"
                keywords_info[i] = {'keyword':idea.text, 'inspired_by':keyword[0], 'competition_level':competition_value,
                                    'low_price':low_price, 'high_price':high_price, 'traffic':traffic,
                                    'avg_monthly_traffic':avg_monthly_traffic, 'date':date, 'date_update':date_update}
                i += 1
        df = pd.DataFrame.from_dict(keywords_info, "index")
        return df

    def _map_locations_ids_to_resource_names(self, client, location_ids):
        """Converts a list of location IDs to resource names.
        Args:
            client: an initialized GoogleAdsClient instance.
            location_ids: a list of location ID strings.
        Returns:
            a list of resource name strings using the given location IDs.
        """
        build_resource_name = client.get_service("GeoTargetConstantService").geo_target_constant_path
        return [build_resource_name(location_id) for location_id in location_ids]

if __name__ == "__main__":

    gad = GoogleAds()
    df = gad.get_keyword_info('湖人')
