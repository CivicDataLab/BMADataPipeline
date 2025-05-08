from sqlalchemy.orm import declarative_base, relationship
from sqlalchemy import Column, Integer, String, Text, Float, DateTime, func, ForeignKey,UniqueConstraint
from sqlalchemy.dialects.postgresql import JSONB
import pendulum

Base = declarative_base()

class BangkokBudget(Base):
    __tablename__ = 'bangkok_budget'
    __table_args__=(
        UniqueConstraint('mis_id',  name='uq_bangkok_budget_mis_id'),
    )
    id = Column(Integer, primary_key=True)
    mis_id = Column(String(255),nullable=True, unique=True, index=True)
    department_name = Column(String(255))
    sector_name = Column(String(255))
    program_name = Column(String(255))
    func_name = Column(String(255))
    expenditure_name = Column(String(255))
    subobject_name = Column(String(255))
    func_id = Column(String(50))
    func_year = Column(String(50))
    func_seq = Column(String(50))
    exp_object_id = Column(String(50))
    exp_subobject_id = Column(String(50))
    expenditure_id = Column(String(50))
    item_id = Column(String(50))
    detail = Column(Text)
    approve_on_hand = Column(String(255))
    allot_approve = Column(String(255))
    allot_date = Column(String(50))
    agr_date = Column(String(50))
    open_date = Column(String(50))
    acc_date = Column(String(50))
    acc_amt = Column(String(255))
    sgn_date = Column(String(50))
    st_sgn_date = Column(String(50))
    end_sgn_date = Column(String(50))
    last_rcv_date = Column(String(50))
    vendor_type_id = Column(String(50))
    vendor_no = Column(String(50))
    vendor_description = Column(String(255))
    pay_total_amt = Column(String(255))
    fin_dept_amt = Column(String(255))
    net_amt = Column(Float)
    pur_hire_status = Column(String(50))
    pur_hire_status_name = Column(String(255))
    contract_id = Column(String(50))
    purchasing_department = Column(String(255))
    contract_name = Column(Text)
    contract_amount = Column(String(255))
    pur_hire_method = Column(String(255))
    egp_project_code = Column(String(50))
    egp_po_control_code = Column(String(50))
    original_text = Column(Text)
    translation = Column(Text)
    entities = Column(JSONB)
    geocoding_of = Column(String(255))
    lat = Column(Float)
    lon = Column(Float)
    created_at = Column(  DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now())
    updated_at = Column( DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now())
    raw_data = Column(JSONB)



class RainfallSensor(Base):
    __tablename__ = 'rainfall_sensor'

    id = Column(Integer, primary_key=True)
    code = Column(String(255))
    name=Column('name', String(255))
    district=Column('district', String(255))
    latitude=Column('latitude', String(255))
    longitude=Column('longitude', String(255))
    created_at=Column('created_at', DateTime(timezone=True),
                   server_default=func.now()),
    updated_at=Column('updated_at', DateTime(timezone=True),
                   server_default=func.now(), onupdate=func.now())

    # one-to-many → streaming data & risk points
    streaming_data = relationship(
        "RainfallSensorStreamingData",
        back_populates="rainfall_sensor",
        cascade="all, delete-orphan",
    )
    risk_points    = relationship(
        "RiskPoint",
        back_populates="rainfall_sensor",
    )


class RainfallSensorStreamingData(Base):
    __tablename__ = 'rainfall_sensor_streaming_data'

    id         = Column(Integer, primary_key=True)
    code       = Column(String(255))
    sensor_id  = Column(
        Integer,
        ForeignKey(
            'rainfall_sensor.id',
            ondelete='SET NULL',
            onupdate='CASCADE'
        ),
        nullable=True
    )
    rf5min=Column(Float, nullable=True)
    rf15min=Column(Float, nullable=True)
    rf30min=Column(Float, nullable=True)
    rf1hr=Column(Float, nullable=True)
    rf3hr=Column(Float, nullable=True)
    rf6hr=Column(Float, nullable=True)
    rf12rh=Column(Float, nullable=True)
    rf24rh     = Column(Float,   nullable=True)
    site_time  = Column(DateTime(timezone=True), nullable=True)
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    rainfall_sensor = relationship(
        "RainfallSensor",
        back_populates="streaming_data",
    )


class FloodSensor(Base):
    __tablename__ = 'flood_sensor'

    id         = Column(Integer, primary_key=True)
    code       = Column(String(255))
    name       = Column(String(255))
    road       = Column(String(255))
    district   = Column(String(255))
    lat        = Column(String(255))
    long       = Column(String(255))
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # one-to-many → risk points
    risk_points = relationship(
        "RiskPoint",
        back_populates="road_flood_sensor",
    )


class RiskPoint(Base):
    __tablename__ = 'risk_points'

    id                             = Column(Integer, primary_key=True)
    objectid                       = Column(Integer)
    rainfall_sensor_id             = Column(
        Integer,
        ForeignKey(
            'rainfall_sensor.id',
            ondelete='SET NULL',
            onupdate='CASCADE'
        ),
        nullable=True
    )
    closest_rainfall_sensor_code   = Column(String(255))
    closest_rainfall_sensor_distance = Column(Float)
    road_flood_sensor_id           = Column(
        Integer,
        ForeignKey(
            'flood_sensor.id',
            ondelete='SET NULL',
            onupdate='CASCADE'
        ),
        nullable=True
    )
    closest_road_flood_sensor_code = Column(String(255))
    closest_road_flood_sensor_distance = Column(Float)
    risk_name                      = Column(String(255))
    problems                       = Column(String(255))
    district_t                     = Column(String(255))
    group_t                        = Column(String(255))
    status_num                     = Column(Integer)
    status_det                     = Column(String(255))
    long                           = Column(String(255))
    lat                            = Column(String(255))
    created_at                     = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at                     = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

    # relationships back to parent sensors
    rainfall_sensor   = relationship(
        "RainfallSensor",
        back_populates="risk_points",
    )
    road_flood_sensor = relationship(
        "FloodSensor",
        back_populates="risk_points",
    )


class CanalDredgingProgress(Base):
    __tablename__ = 'canal_dredging_progress'

    id                                 = Column(Integer, primary_key=True)
    record_number                      = Column(Integer)
    district_agency                    = Column(String(255))
    total_canals_assigned              = Column(Integer)
    total_length_assigned_m            = Column(Integer)
    regularly_maintained_canals        = Column(Integer)
    regularly_maintained_length_m      = Column(Integer)
    periodically_flowed_canals         = Column(Integer)
    periodically_flowed_length_m       = Column(Integer)
    total_maintained_flowed_length_m   = Column(Integer)
    percent_maintained_flowed          = Column(Float)
    planned_dredging_canals            = Column(Integer)
    planned_dredging_length_m          = Column(Integer)
    annual_budgeted_canals             = Column(Integer)
    annual_budgeted_length_m           = Column(Integer)
    annual_budget                      = Column(Integer)
    annual_committed_budget            = Column(String(255))
    dredged_length_annual_budget_m     = Column(Integer)
    percent_dredged_annual_budget      = Column(Float)
    additional_budgeted_canals         = Column(Integer)
    additional_budgeted_length_m       = Column(Integer)
    supplementary_budget               = Column(Integer)
    supplementary_committed_budget     = Column(Integer)
    dredged_length_supplementary_budget_m = Column(Integer)
    percent_dredged_supplementary_budget   = Column(Float)
    total_dredged_canals               = Column(Integer)
    total_percent_dredged              = Column(Float)
    remarks                            = Column(String(255))
    district_code                      = Column(String(50))
    buddhist_year                      = Column(Integer)
    day_period                         = Column(String(50))
    month                              = Column(String(50))
    record_date                        = Column(String(50))
    reporting_date                     = Column(String(50))
    plan_sequence                      = Column(String(50))
    created_at                         = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at                         = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )


class SewerageDredgingProgress(Base):
    __tablename__ = 'sewerage_dredging_progress'

    id                                     = Column(Integer, primary_key=True)
    record_number                          = Column(Integer)
    district_agency                        = Column(String(255))
    total_alleys                           = Column(Integer)
    total_length_m                         = Column(Integer)
    alleys_not_cleaned_this_year           = Column(Integer)
    length_not_cleaned_this_year_m         = Column(Integer)
    alleys_cleaned_this_year               = Column(Integer)
    length_cleaned_this_year_m             = Column(Integer)
    planned_alleys_by_district_labor       = Column(Integer)
    planned_length_by_district_labor_m     = Column(Integer)
    executed_alleys_by_district_labor      = Column(Integer)
    executed_length_by_district_labor_m    = Column(Integer)
    percent_executed_by_district_labor     = Column(Float)
    planned_alleys_by_dop                  = Column(Integer)
    planned_length_by_dop_m                = Column(Integer)
    annual_budgeted_alleys_by_dop          = Column(Integer)
    annual_budgeted_length_by_dop_m        = Column(Integer)
    executed_length_annual_by_dop_m        = Column(Integer)
    percent_executed_annual_by_dop         = Column(Float)
    supplementary_budgeted_alleys_by_dop   = Column(Integer)
    supplementary_budgeted_length_by_dop_m = Column(Integer)
    executed_length_supplementary_by_dop_m = Column(Integer)
    percent_executed_supplementary_by_dop  = Column(Float)
    planned_alleys_by_private              = Column(Integer)
    planned_length_by_private_m            = Column(Integer)
    annual_budgeted_alleys_by_private      = Column(Integer)
    annual_budgeted_length_by_private_m    = Column(Integer)
    executed_length_annual_by_private_m    = Column(Integer)
    percent_executed_annual_by_private     = Column(Float)
    supplementary_budgeted_alleys_by_private = Column(Integer)
    supplementary_budgeted_length_by_private_m = Column(Integer)
    executed_length_supplementary_by_private_m = Column(Integer)
    percent_executed_supplementary_by_private   = Column(Float)
    manhole_cover_repaired                 = Column(Integer)
    grating_repaired                       = Column(Integer)
    curb_repaired                          = Column(Integer)
    remarks                                = Column(String(255))
    district_code                          = Column(String(50))
    buddhist_year                          = Column(Integer)
    day_period                             = Column(String(50))
    month                                  = Column(String(50))
    record_date                            = Column(String(50))
    reporting_date                         = Column(String(50))
    plan_sequence                          = Column(String(50))
    annual_budget_by_private_baht          = Column(Float)
    supplementary_budget_by_private_baht   = Column(Float)
    executed_alleys_annual_by_private      = Column(Integer)
    executed_alleys_supplementary_by_private = Column(Integer)
    annual_budget_by_dop_baht              = Column(Float)
    supplementary_budget_by_dop_baht       = Column(Float)
    executed_alleys_annual_by_dop          = Column(Integer)
    executed_alleys_supplementary_by_dop   = Column(Integer)
    created_at                             = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at                             = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )


class FloodSensorStreamingData(Base):
    __tablename__ = 'flood_sensor_streaming_data'

    id                = Column(Integer, primary_key=True)
    date_created      = Column(DateTime(timezone=True))
    sensor_name       = Column(String(255))
    value             = Column(Float)
    record_time       = Column(DateTime)
    sensor_profile_id = Column(Integer)
    district          = Column(String(255))
    name              = Column(String(255))
    device_status     = Column(String(50))
    created_at                             = Column(
        DateTime(timezone=True),
        server_default=func.now()
    )
    updated_at                             = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now()
    )

class TraffyFondueCitizenComplaint(Base):
    __tablename__='traffy_fondue_citizen_complaint'
    id=Column(Integer, primary_key=True)
    longitude=Column(Float)
    latitude=Column(Float)
    description=Column(Text)
    riskpoint_objectid=Column(Integer)
    address=Column(Text)
    timestamp=Column(DateTime(timezone=True))
    note=Column(Text)
    type=Column(String(255))
    district=Column(String(255))
    state_type_latest=Column(String(255))

# risk_id,risk_name,category,district,flood_sensor_code,flood_code,hour,max_flood,flood_duration_minutes,rain_code,rf1hr
class RainfallAndFloodSummary(Base):
    __tablename__='rainfall_and_flood_summary'
    id=Column(Integer, primary_key=True)
    risk_id=Column(Integer)
    risk_name=Column(String(255))
    category=Column(String(255))
    district=Column(String(255))
    flood_sensor_code=Column(String(255))
    flood_code=Column(String(255))
    hour=Column(DateTime(timezone=True))
    max_flood=Column(Float)
    flood_duration_minutes=Column(Float)
    rainfall_sensor_code=Column(String(255))
    rf1hr=Column(Float)
    
# test table, delete after testing

class RainAndFloodSummary2(Base):
    __tablename__='rainfall_and_flood_summary_2'
    id=Column(Integer, primary_key=True)
    risk_id=Column(Integer)
    risk_name=Column(String(255))
    category=Column(String(255))
    district=Column(String(255))
    flood_sensor_code=Column(String(255))
    flood_code=Column(String(255))
    hour=Column(DateTime(timezone=True))
    max_flood=Column(Float)
    flood_duration_minutes=Column(Float)
    flood_drain_rate_in_cm_minutes=Column(Float)
    rainfall_sensor_code=Column(String(255))
    rf1hr=Column(Float)