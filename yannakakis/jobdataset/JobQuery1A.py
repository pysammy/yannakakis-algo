class JobQuery1A:
    def __init__(self):
        self.columns = {
        "movie_info_idx": ["id", "movie_id", "info_type_id", "info", "note"],
        "company_type": ["id", "kind"],
        "movie_companies": ["id", "movie_id", "company_id", "company_type_id", "note"],
        "info_type": ["id", "info"],
        "title": ["id", "title", "imdb_index", "kind_id", "production_year", "imdb_id", "phonetic_code", "episode_of_id", "season_nr", "episode_nr", "series_years", "md5sum"]
        }
        
        self.selection_criteria = {
        "movie_companies": [{"column": "note", "operator": "not like", "value": "%(as Metro-Goldwyn-Mayer Pictures)%"}],
        "movie_companies": [{"column": "note", "operator": "like", "value": "%(co-production)%"}],
        "company_type": [{"column": "kind", "operator": "==", "value": "production companies"}],
        "info_type": [{"column": "info", "operator": "==", "value": "top 250 rank"}]
        }

        self.projection_criteria = {}

        self.join_tree = [
        {"left": "company_type", "right": "movie_companies", "left_key": "id", "right_key": "company_type_id"},
        {"left": "movie_companies", "right": "title", "left_key": "movie_id", "right_key": "id"},
        {"left": "title", "right": "movie_info_idx", "left_key": "id", "right_key": "movie_id"},
        {"left": "movie_companies", "right": "movie_info_idx", "left_key": "movie_id", "right_key": "movie_id"},
        {"left": "movie_info_idx", "right": "info_type", "left_key": "info_type_id", "right_key": "id"}
        ]
        
        self.query = """SELECT COUNT(*) FROM 
        company_type AS ct, info_type AS it, movie_companies AS mc, movie_info_idx AS mi_idx, title AS t WHERE 
        ct.kind = 'production companies' AND it.info = 'top 250 rank' AND mc.note not like '%(as Metro-Goldwyn-Mayer Pictures)%' 
        and (mc.note like '%(co-production)%') AND ct.id = mc.company_type_id AND 
        t.id = mc.movie_id AND t.id = mi_idx.movie_id AND mc.movie_id = mi_idx.movie_id AND it.id = mi_idx.info_type_id;"""