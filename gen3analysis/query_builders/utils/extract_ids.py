from typing import List, Dict


def extract_ids_from_hit(results):
    return [hit["_id"] for hit in results["hits"]["hits"]._l_]
