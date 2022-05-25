from models import session, Affiliations as affiliations_table
from sentence_transformers import util
import numpy as np
import json
from magniv.core import task

THRESHOLD = 0.80


@task(schedule="@daily", description="Daily task to resolve new entites")
def daily_entity_resolution():
    new_affiliations = (
        session.query(affiliations_table).filter_by(merged=False).limit(10).all()
    )
    for affiliation in new_affiliations:
        # loop over all affiliations
        highest_cos_sim = 0
        highest_id = -1
        completed_affiliations = (
            session.query(affiliations_table).filter_by(merged=True).all()
        )
        for c_affiliation in completed_affiliations:
            cos_sim = util.cos_sim(
                np.array(json.loads(affiliation.embedding)),
                np.array(json.loads(c_affiliation.embedding)),
            ).item()
            if cos_sim > THRESHOLD:
                if cos_sim > highest_cos_sim:
                    highest_cos_sim = cos_sim
                    highest_id = c_affiliation.id
        if highest_cos_sim > 0:
            # delete the current
            highest_match = (
                session.query(affiliations_table).filter_by(id=highest_id).first()
            )
            print("START_______________________________________")
            print(affiliation.affiliate_string)
            print("_______________________________________")
            print(highest_match.affiliate_string)
            print("_______________________________________")
            session.delete(affiliation)
            session.commit()
        else:
            print("NO MATCH: ", affiliation.affiliate_string)
            affiliation.merged = True
            session.commit()
        # if no threshold set completed to true


if __name__ == "__main__":
    nightly_task()
