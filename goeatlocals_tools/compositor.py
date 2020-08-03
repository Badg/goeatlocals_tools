import asyncpg
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
                    display_class = None
                    status = None
                    address = None
                    website = None
                    phone = None

                    # TODO: instead of compositing things here, we should
                    # really just queue these up for import in an SQS queue. At
                    # that point we should probably also transition into a COPY
                    # (ie conn.copy_records_to_table) strategy instead of this,
                    # since that would be much more efficient

                    # TODO: we need to do some matching here to determine if we
                    # have an existing row, since we don't know the place_id.
                    # The UNIQUE constraint on osm_id *should* suffice for now,
                    # but it's probably a bit fragile
                    result_record, = await conn.fetch(
                        '''INSERT INTO app_placedata.places (
                            osm_id,
                            identity_name,
                            identity_display_class,
                            status,
                            locator_point,
                            locator_address,
                            locator_website,
                            locator_phone)
                        SELECT $1, $2, $3, $4, src_table.geometry, $5, $6, $7
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

            # result = await conn.fetchval("SELECT 'a=>1,b=>2'::hstore")
            # assert result == {'a': 1, 'b': 2}

            # data = shapely.geometry.Point(-73.985661, 40.748447)
            # res = await conn.fetchrow(
            #     '''SELECT 'Empire State Building' AS name,
            #               $1::geometry            AS coordinates
            #     ''',
            #     data)


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
