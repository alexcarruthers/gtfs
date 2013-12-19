import sqlalchemy
from entity import *


class Schedule:
    def __init__(self, db_filename):
        self.db_filename = db_filename

        self.engine = sqlalchemy.create_engine('sqlite:///%s' % self.db_filename, echo=False)
        Session = sqlalchemy.orm.sessionmaker(bind=self.engine)
        self.session = Session()

    @property
    def routes(self):
        return self.session.query(Route).all()

    @property
    def agencies(self):
        return self.session.query(Agency).all()

    @property
    def service_periods(self):
        return self.session.query(ServicePeriod).all()

    @property
    def stops(self):
        return self.session.query(Stop).all()

    def create_tables(self):
        metadata.create_all(self.engine)

    @property
    def trips(self):
        return self.session.query(Trip).all()

    @property
    def stop_times(self):
        return self.session.query(StopTime).all()

    @property
    def fares(self):
        return self.session.query(FareRule).all()

    def fare_rule(self, route_id, origin_id, destination_id):
        fare_rule = self.session.query(FareRule) \
            .filter(FareRule.route_id == route_id)\
            .filter(FareRule.origin_id == origin_id)\
            .filter(FareRule.destination_id == destination_id) \
            .first()

        return fare_rule

    def fare_info(self, fare_id):
        return self.session.query(Fare).filter(Fare.fare_id == fare_id).one()

    def trip_stops(self, trip_id):
        stops = self.session.query(StopTime).filter(
            StopTime.trip_id == trip_id
        ).all()

        return stops

    def calendar(self, service_id):
        return self.session.query(ServicePeriod).filter(
            ServicePeriod.service_id == service_id
        ).first()