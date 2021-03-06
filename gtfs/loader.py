from schedule import Schedule
import feed
from entity import *
import sys


def load(feed_filename, db_filename=":memory:"):
    schedule = Schedule(db_filename)
    schedule.create_tables()

    fd = feed.Feed(feed_filename)

    for gtfs_class in (Agency,
                       Route,
                       Stop,
                       Trip,
                       StopTime,
                       ServicePeriod,
                       ServiceException,
                       Fare,
                       FareRule,
                       ShapePoint,
                       Frequency,
                       Transfer,
    ):

        try:
            for i, record in enumerate(fd.get_table(gtfs_class.TABLENAME + ".txt")):
                if i % 500 == 0:
                    schedule.session.commit()

                instance = gtfs_class(**record.to_dict())
                schedule.session.add(instance)
        except KeyError:
            # TODO: check if the table is required
            continue

    schedule.session.commit()

    return schedule
