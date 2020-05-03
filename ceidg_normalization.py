import dataclasses
import logging
import google.cloud.logging
from dacite import from_dict
from google.cloud import firestore
import gc

from src.models.ceidg_record import InformacjaOWpisie

MAXIMUM_FIRESTORE_BATCH_SIZE = 500
PROJECT_ID = "simplify-docs"
CEIDG_COLLECTION = "ceidg-prod"
CEIDG_NORMALIZED_COLLECTION = "ceidg-normalized"
logFormatter = logging.Formatter("%(asctime)s [%(threadName)-12.12s] [%(levelname)-5.5s]  %(message)s")
rootLogger = logging.getLogger()

fileHandler = logging.FileHandler("output.log")
fileHandler.setFormatter(logFormatter)
rootLogger.addHandler(fileHandler)

consoleHandler = logging.StreamHandler()
consoleHandler.setFormatter(logFormatter)
rootLogger.addHandler(consoleHandler)

client = google.cloud.logging.Client()
client.setup_logging()


def count_collection(coll_ref, count, cursor=None):
    if cursor is not None:
        docs = [snapshot.reference for snapshot
                in coll_ref.limit(1000).order_by("__name__").start_after(cursor).stream()]
    else:
        docs = [snapshot.reference for snapshot
                in coll_ref.limit(1000).order_by("__name__").stream()]

    count = count + len(docs)

    if len(docs) == 1000:
        return count_collection(coll_ref, count, docs[999].get())
    else:
        print(count)


def normalize_table(client, source_ceidg_collection_ref, target_ceidg_collection_ref, batch_size=5000, cursor=None,
                    counter=0):
    if cursor is not None:
        rootLogger.info("Cursor: {}".format(cursor.id))
        result = source_ceidg_collection_ref.limit(batch_size).order_by("__name__").start_after(cursor).stream()
    else:
        result = source_ceidg_collection_ref.limit(batch_size).order_by("__name__").stream()
    rootLogger.info("batch")
    results = []
    for doc in result:
        results.append(doc)
    total_index = 0
    while total_index < len(results):
        i = 0
        batch = client.batch()
        while i < MAXIMUM_FIRESTORE_BATCH_SIZE and total_index < len(results):
            batch.set(target_ceidg_collection_ref.document(results[total_index].id),
                      normalize(results[total_index].to_dict()))
            i += 1
            total_index += 1
        batch.commit()
        counter += i
        rootLogger.info("Processed documents: {}".format(counter))
    if len(results) != batch_size or counter >= 5000000:
        return
    cursor = results[len(results) - 1]
    # Remove references to reduce memory usage
    results = []
    result = []
    gc.collect()
    normalize_table(client, source_ceidg_collection_ref, target_ceidg_collection_ref, cursor=cursor,
                    counter=counter)


def normalize(record):
    convert_to_list('Zakazy', 'InformacjaOZakazie', record)
    convert_to_list('SpolkiCywilneKtorychWspolnikiemJestPrzedsiebiorca', 'InformacjeOSpolce', record)
    convert_to_list('InformacjeDotyczaceUpadlosciPostepowaniaNaprawczego', 'Informacja', record)
    convert_to_list('Uprawnienia', 'Uprawnienie', record)
    convert_to_list('DaneAdresowe', 'AdresyDodatkowychMiejscWykonywaniaDzialalnosci', record)
    return dataclasses.asdict(from_dict(data_class=InformacjaOWpisie, data=record))


def convert_to_list(field_name, nested_field_name, record):
    if field_name == 'DaneAdresowe':
        if nested_field_name in record[field_name] and record[field_name][
            nested_field_name] is not None and \
                isinstance(record[field_name][nested_field_name]['Adres'], dict):
            record[field_name][nested_field_name]['Adres'] = [
                record[field_name][nested_field_name]['Adres']]
        return record
    if should_be_converted(record, field_name, nested_field_name):
        record[field_name][nested_field_name] = [record[field_name][nested_field_name]]
    return record


def should_be_converted(record, field_name, nested_field_name):
    return field_name in record and record[field_name] is not None and len(record[field_name]) == 1 and (
            isinstance(record[field_name][nested_field_name],
                       dict) or isinstance(record[field_name][nested_field_name], str))


if __name__ == '__main__':
    c = firestore.Client(PROJECT_ID)
    ceidg_collection_ref = c.collection(CEIDG_COLLECTION)
    target_collection_ref = c.collection(CEIDG_NORMALIZED_COLLECTION)
    normalize_table(c, ceidg_collection_ref, target_collection_ref)
