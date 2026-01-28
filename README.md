# Monarch

Monarch is an Oban job powered process for automatically running data migrations.

## Installation

The package can be installed by adding `monarch` to your list of dependencies in `mix.exs`:

```elixir
def deps do
  [
    {:monarch, "~> 0.1.0"}
  ]
end
```

### Setting up Your Database

Monarch will discover Oban jobs that need to be run and queue them up for you.
It will only attempt to queue up jobs that have not already been completed using Monarch.

This library uses a database table, `monarch_jobs` to determine if an Oban job has already been completed.
In order for this to work properly, create a new migration against your app using the mix command: `mix ecto.gen.migration add_monarch_jobs`.
Your migration should spin up the `monarch_jobs` table needed like this:

```
use Ecto.Migration

def up, do: Monarch.Migrations.up(version: 1)
def down, do: Monarch.Migrations.down(version: 1)
```

### Implementing the Monarch behaviour

In order to write a Monarch job, you need to implement the Monarch behaviour in a module inside your application. Monarch will automatically detect this at runtime.

You can use this mix task inside of Monarch to spin up a module for you or write one manually.

`mix monarch --monarch-path apps/myapp/lib/myapp/workers/monarch my_monarch_module`

This should create the directory from the `monarch-path` if it doesn't already exist and create the `my_monarch_module` file inside the direcotry with a template of the Monarch behaviour implemented for you.

Then, there are 4 functions that should be generated that you need for our Monarch behaviour to work: a `scheduled_at/0`, a `skip/0` a `query/0` function and an `update/1` function.

- `scheduled_at/0` - The date and time the job should be run in UTC. This should work the same way as the implementation of a normal Oban job. If the `scheduled_at` time is in the past, the job will automatically be queued and executed when Monarch is next ran. If `scheduled_at` is nil, the job will not be automatically enqueued and should be manually run. If the `scheduled_at` is in the future, the job will be executed at the time specified.
- `skip/0` - Specifies whether to skip executing the job. Skipping will mark the Monarch job as complete but will not actually run what is specified in the module. This is useful for example if you want to run a particular job only on certain environments. You could specify: `Application.get_env(:monarch, Monarch)[:deploy_environment] != :production` in a Monarch behaviour module and it would skip executing Monarch jobs that are not production but still mark them as complete in the current environment so they are not attempted to run again.
- `query/0` - Should return the list of records that need to be updated.
- `update/1` - Takes the list of records from `query/0` and performs the given update.

There are currently three optional callbacks:

- `snooze?/0` - If this function is implemented, and returns an integer, the job will be rescheduled to run again in that many seconds. This is useful if you want to run a job again after a certain amount of time has passed. Does nothing if falsey or unimplemented.
- `transaction?/0` - Controls whether or not the job should be run in a transaction. If this function is implemented and returns true, the job will be run in a transaction. If falsey or unimplemented, the job will not be run in a transaction.
- `queue/0` - Specifies which Oban queue the job should be enqueued to. If this function is implemented and returns a string, the job will be inserted into that queue. If unimplemented, the job will use the default queue passed to `Monarch.run/2`.

Monarch will keep running until `query/0` returns no remaining records to be updated, after which it will record a completed job in the `monarch_jobs` table.

For example, let's say you add a new column `verified_email` on a `Users` schema that is defaulted to `nil` and want to backfill all existing users to have this set to be false.

You query could look like this:

```
def query do
  User
  |> where([user], is_nil(user.verified_email))
  |> limit(500)
  |> select(user.id)
  |> MyApp.Repo.all()
end
```

This will only return a list of user ID records that have not had there `verified_email` column set yet in batches of 500.

Then, our `update/1` function could look like this:

```
def update(user_ids) do
  MyApp.Repo.update_all(
    from(user in User,
      where: user.id in ^user_ids,
      update: [set: [verified_email: false]]
    ),
    []
  )
end
```

This takes in the `user_ids` returned from our query function and uses them to perform a batch update on all those users to have their `verified_email` column set to false.

Finally, if there are still more users that need to be updated Monarch will rerun the query to verify and reperform the job until there are no more records that need to be updated.

### Running Monarch

You can run Monarch manually via the command line or include it in your project's application module so it is automatically run _after_ your application has been started and Oban is up and running.

All you need to run is `Monarch.run(Oban, #{queue_name})`.

`Oban` should be your own instance of Oban inside your application you pass into Monarch and `#{queue_name}` is any queue you have defined that you want jobs to be run against. Your queue can be an existing queue your application is using or you can create a queue specifically for Monarch.

Please see [Oban's documentation](https://hexdocs.pm/oban/Oban.html) on how to install Oban and define queues.
