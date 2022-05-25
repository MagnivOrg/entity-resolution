import pandas as pd
import json
from sentence_transformers import SentenceTransformer
from models import session, Affiliations as affiliations_table



def prepare():
    affiliations = pd.read_csv("./tasks/datas/affiliationstrings_ids.csv")
    shuffled_affiliations = affiliations.sample(frac=1)
    training_set = shuffled_affiliations[0:shuffled_affiliations.shape[0]//2]
    testing_set = shuffled_affiliations[shuffled_affiliations.shape[0]//2:-1]
    model = SentenceTransformer('sentence-transformers/all-mpnet-base-v2')

    # Go through them and put them into their corresponding buckets --- then put it in a table with id,sentence,embedding,new

    included_ids = {}
    for i, row in training_set.iterrows():
        included_ids[row["id1"]] = i


    affiliations_mappings = pd.read_csv("./tasks/datas/affiliationstrings_mapping.csv", header=None)
    lookup_table = {}
    for row in affiliations_mappings.iterrows():
        if row[1][0] in lookup_table:
            lookup_table[row[1][0]].append(row[1][1])
        else:
            lookup_table[row[1][0]] = [row[1][1]]

    for i,affiliation in testing_set.iterrows():
        affiliate_id = affiliation["id1"]
        affiliate_string = affiliation["affil1"]
        embedding = model.encode(affiliate_string)
        new_affiliation = affiliations_table(
            affiliate_id = affiliate_id,
            affiliate_string = affiliate_string,
            embedding = json.dumps(embedding.tolist()),
            merged=False
        )
        session.add(new_affiliation)
        session.commit()

    for affiliate_id in included_ids:
        affiliate_string = affiliations.iloc[included_ids[affiliate_id]]["affil1"]
        affiliation_matches = lookup_table[affiliate_id]
        for match in affiliation_matches:
            if match in included_ids:
                print("yes included")
                affiliate_string = "{}\n{}".format(affiliate_string, affiliations.iloc[included_ids[match]]["affil1"])
            else:
                print("no not included")
        embedding = model.encode(affiliate_string)
        print("embedding ", embedding) 
        # put into the DB
        print("match ", match, " ", affiliate_string)
        new_affiliation = affiliations_table(
            affiliate_id=affiliate_id,
            affiliate_string=affiliate_string,
            embedding = json.dumps(embedding.tolist()),
            merged=True
        )
        session.add(new_affiliation)
        session.commit()

if __name__ == "__main__":
    prepare()
