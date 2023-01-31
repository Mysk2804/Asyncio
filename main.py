import asyncio
from aiohttp import ClientSession
from more_itertools import chunked
import datetime
from db import engine, Session, People, Base
from pprint import pprint

MAX_SIZE = 10


async def paste_to_bd(people_list):
    async with Session() as session:
        people_list_orm = [People(
            id=item['id'],
            name=item['name'],
            height=item['height'],
            mass=item['mass'],
            hair_color=item['hair_color'],
            skin_color=item['skin_color'],
            eye_color=item['eye_color'],
            birth_year=item['birth_year'],
            gender=item['gender'],
            homeworld=item['homeworld'],
            films=item['films'],
            starships=item['starships'],
            vehicles=item['vehicles'],
            species=item['species']
        ) for item in people_list]
        session.add_all(people_list_orm)
        await session.commit()
        print(people_list[-1].get('url'))


async def get_items(json_data, client, name):
    json_data_new = []
    for item in json_data:
        film_name = await client.get(item)
        add_film = await film_name.json()
        json_data_new.append(add_film[name])
    json_data_new = ", ".join(json_data_new)
    return json_data_new


async def get_people(people_id: int, client: ClientSession):
    url = f'https://swapi.dev/api/people/{people_id}/'
    try:
        async with client.get(url) as response:
            json_data = await response.json()
            json_data['id'] = people_id
            json_data_world = await client.get(json_data['homeworld'])
            json_data_world = await json_data_world.json()
            json_data['homeworld'] = json_data_world['name']
            json_data['vehicles'] = await get_items(json_data=json_data['vehicles'], client=client, name='name')
            json_data['species'] = await get_items(json_data=json_data['species'], client=client, name='name')
            json_data['films'] = await get_items(json_data=json_data['films'], client=client, name='title')
            json_data['starships'] = await get_items(json_data=json_data['starships'], client=client, name='name')
            # pprint(json_data)
        return json_data
    except Exception:
        print('KeyError')


async def main():

    tasks = []
    async with ClientSession() as session:
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.drop_all)
            await conn.run_sync(Base.metadata.create_all)

        for id_chunk in chunked(range(1, 100), MAX_SIZE):
            coros = [get_people(people_id=people_id, client=session) for people_id in id_chunk]
            people_list = await asyncio.gather(*coros)
            db_coro = paste_to_bd(people_list)
            paste_to_db_task = asyncio.create_task(db_coro)
            tasks.append(paste_to_db_task)
    tasks = asyncio.all_tasks() - {asyncio.current_task()}
    for task in tasks:
        await task


start = datetime.datetime.now()
asyncio.run(main())
print(datetime.datetime.now() - start)

