import os


def get_env_int(name, default=1):
    return int(os.environ.get(name, default))

def generate_docker_compose():
    template = """
name: tp1
services:
  rabbitmq:
    image: rabbitmq:management
    container_name: rabbitmq
    ports:
      - 5672:5672 # For Clients AMQP
      - 15672:15672 # For RabbitMQ Management UI
    networks:
      - movies_net
    logging:
      driver: none

  server:
    build:
      context: .
      dockerfile: server/Dockerfile
    environment:
      - CLI_LOG_LEVEL=info
      - SERVER_PORT=3000
      - SERVER_LISTEN_BACKLOG=1
      - CLI_WORKER_EXCHANGE_INPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=movies.first.input,credits.input,ratings.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
    container_name: server
    image: server:latest
    entrypoint: python main.py
    networks:
      - movies_net
    ports:
      - 3000:3000
    depends_on:
      - rabbitmq
    logging:
      driver: none
    """
    filter_argq1 = get_env_int("filter_argq1")
    filter_argq3 = get_env_int("filter_argq3")
    filter_argq4 = get_env_int("filter_argq4")
    filter_spa = get_env_int("filter_spa")
    filter_afterq3 = get_env_int("filter_afterq3")
    filter_afterq4 = get_env_int("filter_afterq4")
    filter_only = get_env_int("filter_only")

    group_by_actor_num = get_env_int("group_actor")
    group_by_country_num = get_env_int("group_country")
    group_by_movie_num = get_env_int("group_movie")
    group_by_overview_num = get_env_int("group_overview")

    join_rating = get_env_int("join_rating")
    join_credit = get_env_int("join_credit")
    machine_learning_num = get_env_int("machine_learning")

    filter_argentinaq1 = """
  filter-argentinaq1{arg}:
    build:
      context: .
      dockerfile: filters/filter_argentina/Dockerfile
    container_name: filter-argentinaq1{arg}
    image: filter-argentina:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies.first.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={spa}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF=1
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    routing_key_spain = []
    for i in range (1, filter_spa + 1):
        routing_key_spain.append(f"movies{i}.spain_2000.input")
    for i in range(1, filter_argq1 + 1):
        result = ",".join(routing_key_spain)
        template += filter_argentinaq1.format(arg=i, spa=result)

    filter_argentinaq3 = """
  filter-argentinaq3{arg}:
    build:
      context: .
      dockerfile: filters/filter_argentina/Dockerfile
    container_name: filter-argentinaq3{arg}
    image: filter-argentina:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies.first.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={afterq3}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF=1
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    routing_key_arg_q3 = []
    for i in range (1, filter_afterq3 + 1):
        routing_key_arg_q3.append(f"filter{i}.made_after_2000q3.input")
    for i in range(1, filter_argq3 + 1):
        result = ",".join(routing_key_arg_q3)
        template += filter_argentinaq3.format(arg=i, afterq3=result)

    filter_argentinaq4 = """
  filter-argentinaq4{arg}:
    build:
      context: .
      dockerfile: filters/filter_argentina/Dockerfile
    container_name: filter-argentinaq4{arg}
    image: filter-argentina:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies.first.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={afterq4}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF=1
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    routing_key_arg_q4 = []
    for i in range (1, filter_afterq4 + 1):
        routing_key_arg_q4.append(f"filter{i}.made_after_2000q4.input.input")
    for i in range(1, filter_argq4 + 1):
        result = ",".join(routing_key_arg_q4)
        template += filter_argentinaq4.format(arg=i, afterq4=result)

    filter_spain = """
  filter-spain-2000{spa}:
    build:
      context: .
      dockerfile: filters/filter_spain_2000/Dockerfile
    container_name: filter-spain-2000{spa}
    image: filter-spain-2000:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies{spa}.spain_2000.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF={argLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    
    for i in range(1, filter_spa + 1):
        template += filter_spain.format(spa=i, argLen=filter_argq1)

    filter_2000_q3 = """
  filter-after-2000-q3{afterq3}:
    build:
      context: .
      dockerfile: filters/filter_after_2000/Dockerfile
    container_name: filter-after-2000-q3{afterq3}
    image: filter-after-2000:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=filter{afterq3}.made_after_2000.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={joinr}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF={argLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    routing_key_joinr = []
    for i in range (1, join_rating + 1):
        routing_key_joinr.append(f"join_movies_rating{i}.input")
    for i in range(1, filter_afterq3 + 1):
        result = ",".join(routing_key_joinr)
        template += filter_2000_q3.format(afterq3=i, argLen=filter_argq3, joinr=result)

    filter_2000_q4 = """
  filter-after-2000-q4{afterq4}:
    build:
      context: .
      dockerfile: filters/filter_after_2000/Dockerfile
    container_name: filter-after-2000-q4{afterq4}
    image: filter-after-2000:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=movies_filtered_by_argentina_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=filter{afterq4}.made_after_2000.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={joinc}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF={argLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    routing_key_joinc = []
    for i in range (1, join_credit + 1):
        routing_key_joinc.append(f"join_movies_credit{i}.input")
    for i in range(1, filter_afterq4 + 1):
        result = ",".join(routing_key_joinc)
        template += filter_2000_q4.format(afterq4=i, argLen=filter_argq4, joinc=result)

    filter_only_one = """
  filter-only-one-country{one}:
    build:
      context: .
      dockerfile: filters/filter_only_one_country/Dockerfile
    container_name: filter-only-one-country{one}
    image: filter-only-one-country:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={groupc}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_EXPECTEDEOF=1
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    routing_key_groupc = []
    for i in range (1, group_by_country_num + 1):
        routing_key_groupc.append(f"filter{i}.only-one-country.output")
    for i in range(1, filter_only + 1):
        result = ",".join(routing_key_groupc)
        template += filter_only_one.format(one=i, groupc=result)


    group_by_actor = """
  group_by_actor_count{groupa}:
    build:
      context: .
      dockerfile: group_by/group_by_actor_count/Dockerfile
    volumes:
      - ./group_by/group_by_actor_count/main/config.yaml:/config.yaml
    container_name: group_by_actor_count{groupa}
    image: group_by_actor_count:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=join{groupa}.movie-credits.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=groupby.actor.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={joincLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    for i in range(1, group_by_actor_num + 1):
        template += group_by_actor.format(groupa=i, joincLen = join_credit)

    group_by_country = """
  group_by_country_sum{groupc}:
    build:
      context: .
      dockerfile: group_by/group_by_country_sum/Dockerfile
    container_name: group_by_country_sum{groupc}
    image: group_by_country_sum:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=filter{groupc}.only-one-country.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=groupby.country.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={onlyOneLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    for i in range(1, group_by_country_num + 1):
        template += group_by_country.format(groupc=i, onlyOneLen = filter_only)

    group_by_movie = """
  group_by_movie_average{groupm}:
    build:
      context: .
      dockerfile: group_by/group_by_movie_average/Dockerfile
    container_name: group_by_movie_average{groupm}
    image: group_by_movie_average:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=join{groupm}.movie-ratings.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=groupby.movie.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={joinrLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    for i in range(1, group_by_movie_num + 1):
        template += group_by_movie.format(groupm=i, joinrLen = join_rating)


    group_by_overview = """
  group_by_overview_average{groupo}:
    build:
      context: .
      dockerfile: group_by/group_by_overview_average/Dockerfile
    container_name: group_by_overview_average{groupo}
    image: group_by_overview_average:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=ml_group_by_overview_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=group_by_overview{groupo}.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=group_by-master_group_by
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=master_group_by_overview_average.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={mlLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    for i in range(1, group_by_overview_num + 1):
        template += group_by_overview.format(groupo=i, mlLen = machine_learning_num)

    join_movie_credits = """
  join_movie_credits{joinc}:
    build:
      context: .
      dockerfile: joins/join_movie_credits/Dockerfile
    volumes:
      - ./joins/join_movie_credits/main/config.yaml:/config.yaml
    container_name: join_movie_credits{joinc}
    image: join_movie_credits:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=filter{joinc}.after-2000.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=join{groupa}.movie-credits.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={afterLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    routing_key_groupa = []
    for i in range (1, group_by_actor_num + 1):
        routing_key_groupa.append(f"join{i}.movie-credits.output")
    for i in range(1, filter_only + 1):
        result = ",".join(routing_key_groupa)
        template += join_movie_credits.format(joinc=i, afterLen = filter_afterq4, groupa=result)

    join_movie_ratings = """
  join_movie_ratings{joinr}:
    build:
      context: .
      dockerfile: joins/join_movie_ratings/Dockerfile
    volumes:
      - ./joins/join_movie_ratings/main/config.yaml:/config.yaml
    container_name: join_movie_ratings{joinr}
    image: join_movie_ratings:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=filter{joinr}.after-2000.output
      - CLI_WORKER_EXCHANGE_SECONDINPUT_NAME=ratings
      - CLI_WORKER_EXCHANGE_SECONDINPUT_ROUTINGKEYS=ratings.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=join{groupm}.movie-ratings.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={afterLen}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    routing_key_groupm = []
    for i in range (1, group_by_movie_num + 1):
        routing_key_groupm.append(f"join{i}.movie-ratings.output")
    for i in range(1, filter_only + 1):
        result = ",".join(routing_key_groupm)
        template += join_movie_ratings.format(joinr=i, afterLen = filter_afterq3, groupm=result)

    machine_learning = """
  machine_learning{ml}:
    build:
      context: .
      dockerfile: machine_learning/Dockerfile
    container_name: machine_learning{ml}
    image: machine_learning:latest
    volumes:
      - ./machine_learning:/app
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=first_layer_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=movies.first.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=ml_group_by_overview_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS={groupo}
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """

    routing_key_groupo = []
    for i in range (1, group_by_overview_num + 1):
        routing_key_groupo.append(f"group_by_overview{i}.input")
    for i in range(1, filter_argq3 + 1):
        result = ",".join(routing_key_groupo)
        template += machine_learning.format(ml=i, groupo=result)

    master_group_by_actor = """
  master_group_by_actor:
    build:
      context: .
      dockerfile: master_group_by/master_group_by_actor_count/Dockerfile
    container_name: master_group_by_actor_count
    image: master_group_by_actor_count:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=group_by-master_group_by_actor
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=master_group_by_actor_count.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={groupa}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += master_group_by_actor.format(groupa=group_by_actor_num)

    master_group_by_country = """
  master_group_by_country:
    build:
      context: .
      dockerfile: master_group_by/master_group_by_country_sum/Dockerfile
    container_name: master_group_by_country_sum
    image: master_group_by_country_sum:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=group_by-master_group_by_country
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=master_group_by_country_sum.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={groupc}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += master_group_by_country.format(groupc=group_by_country_num)

    master_group_by_movie = """
  master_group_by_movie:
    build:
      context: .
      dockerfile: master_group_by/master_group_by_movie_average/Dockerfile
    container_name: master_group_by_movie_average
    image: master_group_by_movie_average:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=group_by-master_group_by_movie
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=master_group_by_movie_average.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={groupm}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += master_group_by_movie.format(groupm=group_by_movie_num)


    master_group_by_overview = """
  master_group_by_overview_average:
    build:
      context: .
      dockerfile: master_group_by/master_group_by_overview_average/Dockerfile
    container_name: master_group_by_overview_average
    image: master_group_by_overview_average:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=group_by-master_group_by_overview
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=master_group_by_movie_average.input
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=results_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=queries_partial_results.input
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
      - CLI_WORKER_MAXMESSAGES=10
      - CLI_WORKER_EXPECTEDEOF={groupo}
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += master_group_by_overview.format(groupo=group_by_overview_num)

    top_five = """
  top_five_country_budget:
    build:
      context: .
      dockerfile: topn/top_five_country_budget/Dockerfile
    container_name: top_five_country_budget
    image: top_five_country_budget:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=groupby.country.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=topn.country.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += top_five
    top_ten = """
  top_ten_cast_movie:
    build:
      context: .
      dockerfile: topn/top_ten_cast_movie/Dockerfile
    container_name: top_ten_cast_movie
    image: top_ten_cast_movie:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=groupby.actor.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=topn.cast-movie.output
      - CLI_WORKER_BROKER=amqp://guest:guest@rabbitmq:5672/
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += top_ten
    first_and_last = """
  first_and_last:
    build:
      context: .
      dockerfile: topn/first_and_last/Dockerfile
    container_name: first_and_last
    image: first_and_last:latest
    environment:
      - CLI_LOG_LEVEL=info
      - CLI_WORKER_EXCHANGE_INPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_INPUT_ROUTINGKEYS=groupby.movie.output
      - CLI_WORKER_EXCHANGE_OUTPUT_NAME=query_exchange
      - CLI_WORKER_EXCHANGE_OUTPUT_ROUTINGKEYS=topn.first-last.output
      - CLI_WORKER_MAXMESSAGES=10
    entrypoint: ./run_worker
    networks:
      - movies_net
    depends_on:
      - rabbitmq
    """
    template += first_and_last
    

    template += """
networks:
  movies_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
    """

    return template
    
def create_docker_compose_file(output_filename):
    
    docker_compose_data = generate_docker_compose()

    with open(output_filename, "w") as output_file:
        output_file.write(docker_compose_data.strip())


def main():
    
    create_docker_compose_file("docker-mega-compose.yaml")

if __name__ == "__main__":
    main()