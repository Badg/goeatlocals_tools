import dataclasses

import asyncpg
import phonenumbers
import shapely
import trio_asyncio
from trio_asyncio import aio_as_trio


PROD_SCHEMA = 'mapdata_prod'


async def run():
    conn_kwargs = {
        'host': 'goeatlocals_client_web-postgis',
        'database': 'gis',
        'user': 'postgres',
        # TODO: lol not this; not that it'd work in prod anyways
        'password': 'postgres'
    }
    await _run_as_asyncio(conn_kwargs)


@aio_as_trio
async def _run_as_asyncio(conn_kwargs):
    async with asyncpg.create_pool(**conn_kwargs) as conn_pool:
        async with conn_pool.acquire() as conn:
            # Allow us to decode hstores into dicts
            await conn.set_builtin_type_codec(
                'hstore', codec_name='pg_contrib.hstore')
            # await conn.set_type_codec(
            #     # also works for "geography"
            #     'geometry',
            #     encoder=_encode_geometry,
            #     decoder=_decode_geometry,
            #     format='binary',
            # )
            # I *think* this will work for enums, but it's not ideal
            # await conn.set_type_codec('feed_fmt', decoder=str, encoder=str)

            async with conn.transaction():
                async for table, record in _get_relevant_osm_poi_rows(conn):
                    status = None

                    tags = record['tags']
                    mapping_key = record['mapping_key']
                    subclass = record['subclass']

                    display_class = _classify_display_class(
                        mapping_key, subclass, tags)
                    address = _classify_address(
                        mapping_key, subclass, tags)
                    website = _classify_website(
                        mapping_key, subclass, tags)
                    phone = _classify_phone(
                        mapping_key, subclass, tags)

                    # TODO: instead of compositing things here, we should
                    # really just queue these up for import in an SQS queue. At
                    # that point we should probably also transition into a COPY
                    # (ie conn.copy_records_to_table) strategy instead of this,
                    # since that would be much more efficient

                    # TODO: we need a proper matching system to determine if we
                    # have an existing row, since we don't know the place_id.
                    # The UNIQUE constraint on osm_id is better than nothing,
                    # but we still frequently get duplicates.
                    if address is None:
                        result_record = await conn.fetchrow(
                            '''INSERT INTO app_placedata.places (
                                osm_id,
                                identity_name,
                                identity_display_class,
                                status,
                                locator_point,
                                locator_address,
                                locator_website,
                                locator_phone)
                            SELECT $1, $2, $3, $4, src_table.geometry, $5, $6,
                                    $7
                                FROM {src_table} src_table
                                WHERE src_table.osm_id = $1
                            ON CONFLICT (osm_id) DO UPDATE
                                SET identity_name = EXCLUDED.identity_name,
                                    identity_display_class =
                                        EXCLUDED.identity_display_class,
                                    status = EXCLUDED.status,
                                    locator_point = EXCLUDED.locator_point,
                                    locator_address = EXCLUDED.locator_address,
                                    locator_website = EXCLUDED.locator_website,
                                    locator_phone = EXCLUDED.locator_phone
                            RETURNING place_id;
                            '''.format(src_table=table),
                            record['osm_id'],
                            record['name'],
                            display_class,
                            status,
                            address,
                            website,
                            phone)

                    else:
                        result_record = await conn.fetchrow(
                            '''INSERT INTO app_placedata.places (
                                osm_id,
                                identity_name,
                                identity_display_class,
                                status,
                                locator_point,
                                locator_address.street_number,
                                locator_address.street_name,
                                locator_address.unit_number,
                                locator_address.neighborhood,
                                locator_address.city,
                                locator_address.state,
                                locator_address.country,
                                locator_address.postal_code,
                                locator_website,
                                locator_phone)
                            SELECT $1, $2, $3, $4, src_table.geometry,
                                    $5, $6, $7, $8, $9, $10, $11, $12,
                                    $13, $14
                                FROM {src_table} src_table
                                WHERE src_table.osm_id = $1
                            ON CONFLICT (osm_id) DO UPDATE
                                SET identity_name = EXCLUDED.identity_name,
                                    identity_display_class =
                                        EXCLUDED.identity_display_class,
                                    status = EXCLUDED.status,
                                    locator_point = EXCLUDED.locator_point,
                                    locator_address = EXCLUDED.locator_address,
                                    locator_website = EXCLUDED.locator_website,
                                    locator_phone = EXCLUDED.locator_phone
                            RETURNING place_id;
                            '''.format(src_table=table),
                            record['osm_id'],
                            record['name'],
                            display_class,
                            status,
                            address.street_number,
                            address.street_name,
                            address.unit_number,
                            address.neighborhood,
                            address.city,
                            address.state,
                            address.country,
                            address.postal_code,
                            website,
                            phone)

            # result = await conn.fetchval("SELECT 'a=>1,b=>2'::hstore")
            # assert result == {'a': 1, 'b': 2}

            # data = shapely.geometry.Point(-73.985661, 40.748447)
            # res = await conn.fetchrow(
            #     '''SELECT 'Empire State Building' AS name,
            #               $1::geometry            AS coordinates
            #     ''',
            #     data)


@dataclasses.dataclass
class _Address:
    street_number: str
    street_name: str
    unit_number: str
    neighborhood: str
    city: str
    state: str
    country: str
    postal_code: str

    @property
    def completely_useless(self):
        return all(val is None for val in dataclasses.asdict(self).values())


def _classify_display_class(mapping_key, subclass, tags):
    '''Perform normalization logic from OMT's mapping key and subclass,
    and the original OSM tags, to figure out a display class, or, on
    failure, return None.
    '''
    if mapping_key == 'amenity':
        if subclass in {'fast_food', 'food_court', 'restaurant', 'cafe'}:
            return 'prepared_food'
        elif subclass in {'pub', 'biergarten'}:
            return 'light_bar'
        elif subclass in {'bar', 'restaurant'}:
            return 'full_bar'
    elif mapping_key == 'shop':
        if subclass in {
            'greengrocer', 'wholesale', 'supermarket', 'butcher',
            'convenience', 'alcohol', 'wine'
        }:
            return 'grocery'


def _classify_address(mapping_key, subclass, tags):
    '''Perform normalization logic from OMT's mapping key and subclass,
    and the original OSM tags, to figure out an address, or, on
    failure, return None.
    '''
    maybe_address = _Address(
        street_number=tags.get('addr:housenumber'),
        street_name=tags.get('addr:street'),
        unit_number=tags.get('addr:unit'),
        neighborhood=None,
        # TODO: infer city, state, country (when missing; maybe even when not)
        # from the map coordinates
        city=tags.get('addr:city'),
        state=tags.get('addr:state'),
        # TODO: don't assume USA!
        country=tags.get('addr:country'),
        postal_code=tags.get('addr:postcode')
    )

    if maybe_address.completely_useless:
        return None
    else:
        return maybe_address


def _classify_website(mapping_key, subclass, tags):
    '''Perform normalization logic from OMT's mapping key and subclass,
    and the original OSM tags, to figure out a website, or, on
    failure, return None.
    '''
    website_1 = None
    website_2 = None
    website_3 = None

    if 'contact:website' in tags:
        website_1 = tags['contact:website']
    if 'website' in tags:
        website_2 = tags['website']
    if 'url' in tags:
        website_3 = tags['url']

    # This will return whichever is defined, prefering 1, 2, 3 IN THAT ORDER
    # if there are conflicts. This is completely arbitrary. TODO: smarter
    # matching than this! Validation! Fucking anything!
    return (website_1 or website_2 or website_3)


def _classify_phone(mapping_key, subclass, tags):
    '''Perform normalization logic from OMT's mapping key and subclass,
    and the original OSM tags, to figure out a phone number, or, on
    failure, return None.
    '''
    phone_1 = None
    phone_2 = None
    if 'contact:phone' in tags:
        try:
            # TODO: this assumes the USA; it will need to change if we go
            # international. That's easy enough (since we have a map pin!) but
            # it's not as easy as hard-coding 1 in the meantime
            maybe_phone = phonenumbers.parse(tags['contact:phone'], 'US')
        except phonenumbers.NumberParseException:
            pass
        else:
            if (
                phonenumbers.is_possible_number(maybe_phone)
                and phonenumbers.is_valid_number(maybe_phone)
            ):
                phone_1 = maybe_phone

    if 'phone' in tags:
        try:
            # TODO: this assumes the USA; it will need to change if we go
            # international. That's easy enough (since we have a map pin!) but
            # it's not as easy as hard-coding 1 in the meantime
            maybe_phone = phonenumbers.parse(tags['phone'], 'US')
        except phonenumbers.NumberParseException:
            pass
        else:
            if (
                phonenumbers.is_possible_number(maybe_phone)
                and phonenumbers.is_valid_number(maybe_phone)
            ):
                phone_2 = maybe_phone

    # If they're both None, this will still be None. If they're the same,
    # it won't matter; we'll just use one of them. If they're different, we'll
    # prefer the first one, because I'd rather put one up in the hopes of it
    # being right (and therefore useful).
    use_phone = phone_1 or phone_2

    # TODO: store this in the database in a more generalized format
    if use_phone is not None:
        return phonenumbers.format_number(
            use_phone, phonenumbers.PhoneNumberFormat.NATIONAL)


async def _get_relevant_osm_poi_rows(conn):
    async for record in conn.cursor(
        '''SELECT name, osm_id, mapping_key, subclass, tags
        FROM {prod_schema}.osm_poi_point
        WHERE normalized_relevance = TRUE
        '''.format(prod_schema=PROD_SCHEMA)
    ):
        yield f'{PROD_SCHEMA}.osm_poi_point', record

    async for record in conn.cursor(
        '''SELECT name, osm_id, mapping_key, subclass, tags
        FROM {prod_schema}.osm_poi_polygon
        WHERE normalized_relevance = TRUE
        '''.format(prod_schema=PROD_SCHEMA)
    ):
        yield f'{PROD_SCHEMA}.osm_poi_polygon', record


def _encode_geometry(geometry):
    if not hasattr(geometry, '__geo_interface__'):
        raise TypeError('{g} does not conform to '
                        'the geo interface'.format(g=geometry))
    shape = shapely.geometry.asShape(geometry)
    return shapely.wkb.dumps(shape)


def _decode_geometry(wkb):
    return shapely.wkb.loads(wkb)


if __name__ == '__main__':
    trio_asyncio.run(run)
