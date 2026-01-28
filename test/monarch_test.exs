defmodule MonarchTest do
  use Monarch.DataCase, async: true
  use Oban.Testing, repo: Monarch.Repo

  import Ecto.Query

  alias Monarch.Repo

  describe "Monarch.run/1" do
    test "detects and queues jobs" do
      Monarch.run(Monarch.Oban, "test")

      queued_jobs = all_enqueued(worker: Monarch.Worker)

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestEmptyJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker"
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestEmptyJob"
               end)

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestDeleteFakeJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker"
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestDeleteFakeJob"
               end)

      # In this test, "MonarchTestAlreadyCompletedJob" has not already been completed
      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestAlreadyCompletedJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker"
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestAlreadyCompletedJob"
               end)

      scheduled_at_future_time = Timex.end_of_day(DateTime.utc_now())
      scheduled_at_past_time = Timex.beginning_of_day(DateTime.utc_now())

      # A scheduled job will still be enqueued
      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestScheduledFutureJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker",
               scheduled_at: ^scheduled_at_future_time
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestScheduledFutureJob"
               end)

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestScheduledPastJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker",
               scheduled_at: ^scheduled_at_past_time
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestScheduledPastJob"
               end)

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestSkipJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker"
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestSkipJob"
               end)

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestSnoozeJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker"
             } =
               Enum.find(queued_jobs, fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestSnoozeJob"
               end)

      assert 9 = length(queued_jobs)
    end

    test "will not queue a job that has already been completed" do
      Repo.insert_all("monarch_jobs", [
        %{
          inserted_at: NaiveDateTime.truncate(DateTime.utc_now(), :second),
          name: "Elixir.MonarchTestAlreadyCompletedJob"
        }
      ])

      Monarch.run(Monarch.Oban, "test")

      assert 8 = length(all_enqueued(worker: Monarch.Worker))
      refute_enqueued(worker: MonarchTestAlreadyCompletedJob)
    end

    test "will not queue a job that has scheduled_at nil" do
      # Insert a record that would be caught by the job's query
      Repo.insert_all("monarch_jobs", [
        %{
          inserted_at: NaiveDateTime.truncate(DateTime.utc_now(), :second),
          name: "Elixir.AFakeJob"
        }
      ])

      Monarch.run(Monarch.Oban, "test")

      assert 9 = length(all_enqueued(worker: Monarch.Worker))
      refute_enqueued(worker: MonarchTestManualJob)
    end

    for state <- ["available", "scheduled", "executing", "retryable"] do
      test "will not re-enqueue jobs that are currently in state #{state}" do
        # Insert a record that would be caught by the check in monarch
        Repo.insert_all("oban_jobs", [
          %{
            inserted_at: NaiveDateTime.truncate(DateTime.utc_now(), :second),
            worker: "Elixir.Monarch.Worker",
            state: unquote(state),
            args: %{
              "job" => "Elixir.MonarchTestEmptyJob",
              "repo" => "Elixir.Monarch.Repo"
            }
          }
        ])

        Monarch.run(Monarch.Oban, "test")

        # Instead of 9, there should only be 8 jobs enqueued, because the `MonarchTestEmptyJob` job is already running
        assert 8 = length(all_enqueued(worker: Monarch.Worker))
      end
    end

    for state <- ["completed", "discarded", "cancelled"] do
      test "will re-enqueue jobs that are currently in state #{state}" do
        # Insert a record that would be caught by the check in monarch
        Repo.insert_all("oban_jobs", [
          %{
            inserted_at: NaiveDateTime.truncate(DateTime.utc_now(), :second),
            worker: "Elixir.Monarch.Worker",
            state: unquote(state),
            args: %{
              "job" => "Elixir.MonarchTestEmptyJob",
              "repo" => "Elixir.Monarch.Repo"
            }
          }
        ])

        Monarch.run(Monarch.Oban, "test")

        # All jobs are re-enqueued
        assert 9 = length(all_enqueued(worker: Monarch.Worker))
      end
    end

    test "enqueues jobs to custom queue when specified" do
      Monarch.run(Monarch.Oban, "test")

      assert %Oban.Job{
               args: %{
                 "job" => "Elixir.MonarchTestCustomQueueJob",
                 "repo" => "Elixir.Monarch.Repo"
               },
               worker: "Monarch.Worker",
               queue: "custom_queue"
             } =
               [worker: Monarch.Worker]
               |> all_enqueued()
               |> Enum.find(fn oban_job ->
                 oban_job.args["job"] == "Elixir.MonarchTestCustomQueueJob"
               end)
    end
  end

  describe "Monarch.Worker.new/1" do
    test "allows jobs to be enqueued" do
      %{"job" => "Elixir.MonarchTestEmptyJob", "repo" => "Elixir.Monarch.Repo"}
      |> Monarch.Worker.new()
      |> then(&Oban.insert(Monarch.Oban, &1))

      assert 1 = length(all_enqueued(worker: Monarch.Worker))
    end

    test "jobs are unique by their args" do
      %{"job" => "Elixir.MonarchTestEmptyJob", "repo" => "Elixir.Monarch.Repo"}
      |> Monarch.Worker.new()
      |> then(&Oban.insert(Monarch.Oban, &1))

      %{"job" => "Elixir.MonarchTestEmptyJob", "repo" => "Elixir.Monarch.Repo"}
      |> Monarch.Worker.new()
      |> then(&Oban.insert(Monarch.Oban, &1))

      assert 1 = length(all_enqueued(worker: Monarch.Worker))

      %{"job" => "Elixir.MonarchTestEmptyJob2", "repo" => "Elixir.Monarch.Repo"}
      |> Monarch.Worker.new()
      |> then(&Oban.insert(Monarch.Oban, &1))

      assert 2 = length(all_enqueued(worker: Monarch.Worker))
    end
  end

  describe "Monarch.Worker.perform/1" do
    test "inserts a record into `monarch_jobs` when job is completed" do
      assert :ok =
               Monarch.Worker.perform(%Oban.Job{
                 args: %{
                   "job" => "Elixir.MonarchTestEmptyJob",
                   "repo" => "Elixir.Monarch.Repo"
                 },
                 worker: "Monarch.Worker"
               })

      assert 1 =
               from(job in "monarch_jobs",
                 where: job.name == "Elixir.MonarchTestEmptyJob",
                 select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
               )
               |> Repo.all()
               |> length()
    end

    test "detects cycles and discards job" do
      assert {:discard, reason} =
               Monarch.Worker.perform(%Oban.Job{
                 args: %{
                   "job" => "Elixir.MonarchTestCycleJob",
                   "repo" => "Elixir.Monarch.Repo"
                 },
                 worker: "Monarch.Worker"
               })

      assert reason =~ "Cycle detected"
    end

    test "snoozes a job if the worker implements the `snooze?` callback" do
      assert {:snooze, snooze_time} =
               Monarch.Worker.perform(%Oban.Job{
                 args: %{
                   "job" => "Elixir.MonarchTestSnoozeJob",
                   "repo" => "Elixir.Monarch.Repo"
                 },
                 worker: "Monarch.Worker"
               })

      assert snooze_time == 3600
    end

    test "skips a job if the worker implements the `skip?` callback" do
      assert :ok =
               Monarch.Worker.perform(%Oban.Job{
                 args: %{
                   "job" => "Elixir.MonarchTestSkipJob",
                   "repo" => "Elixir.Monarch.Repo"
                 },
                 worker: "Monarch.Worker"
               })

      assert 1 =
               from(job in "monarch_jobs",
                 where: job.name == "Elixir.MonarchTestSkipJob",
                 select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
               )
               |> Repo.all()
               |> length()
    end

    test "recurisvely performs jobs if there are still records to be updated" do
      # Insert a record to be caught by the job's query
      Repo.insert_all("monarch_jobs", [
        %{
          inserted_at: NaiveDateTime.truncate(DateTime.utc_now(), :second),
          name: "Elixir.AFakeJob"
        }
      ])

      assert :ok =
               Monarch.Worker.perform(%Oban.Job{
                 args: %{
                   "job" => "Elixir.MonarchTestDeleteFakeJob",
                   "repo" => "Elixir.Monarch.Repo"
                 },
                 worker: "Monarch.Worker"
               })

      assert 1 =
               from(job in "monarch_jobs",
                 where: job.name == "Elixir.MonarchTestDeleteFakeJob",
                 select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
               )
               |> Repo.all()
               |> length()

      # Verify the job's udpate function was successful and deleted the record
      assert 0 =
               from(job in "monarch_jobs",
                 where: job.name == "Elixir.AFakeJob",
                 select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
               )
               |> Repo.all()
               |> length()
    end
  end

  test "returns an error when module not loaded" do
    job_args = %{
      "job" => "Elixir.MonarchNonExistentJob",
      "repo" => "Elixir.Monarch.Repo"
    }

    result =
      Monarch.Worker.perform(%Oban.Job{
        args: job_args,
        worker: "Monarch.Worker",
        attempt: 1
      })

    assert result == {:error, "Module Elixir.MonarchNonExistentJob not loaded"}
  end
end
