defmodule Monarch.Worker do
  @moduledoc """
  Defines the Monarch Worker for running Oban jobs.
  """

  use Oban.Worker,
    unique: [
      fields: [:args, :worker],
      keys: [:job, :repo]
    ]

  @base_delay 5
  @max_load_attempts 5

  @impl Oban.Worker
  def perform(job) do
    worker = String.to_atom(job.args["job"])
    repo = String.to_atom(job.args["repo"])
    attempt = Map.get(job.args, "load_attempt", 1)

    case ensure_worker_loaded(worker, attempt) do
      :ok ->
        perform_worker_job(clean_job, worker, repo)

      {:retry, next_attempt, delay} ->
        # Update job args with next attempt number for the retry
        updated_job = %{job | args: Map.put(job.args, "load_attempt", next_attempt)}
        {:snooze, delay}

      {:error, reason} ->
        # Max retries exceeded or permanent failure
        {:discard, "Failed to load worker module #{worker}: #{inspect(reason)}"}
    end
  end

  defp perform_worker_job(job, worker, repo) do
    {:ok, {action, resp}} =
      cond do
        worker.skip() ->
          :ok = log_completed!(repo, worker)
          {:ok, {:halt, []}}

        function_exported?(worker, :snooze?, 0) && worker.snooze?() ->
          {:ok, {:snooze, worker.snooze?()}}

        Monarch.completed?(repo, worker) ->
          {:ok, {:halt, []}}

        true ->
          maybe_transaction(worker, repo, fn ->
            case worker.query() do
              [] ->
                :ok = log_completed!(repo, worker)
                {:halt, []}

              chunk ->
                if is_list(job.args["chunk"]) && job.args["chunk"] == chunk do
                  {:discard, chunk}
                else
                  worker.update(chunk)
                  {:cont, chunk}
                end
            end
          end)
      end

    case action do
      # Job finished successfully.
      :halt ->
        :ok

      # Recursively perform the Oban job if there are still records to be updated.
      :cont ->
        perform(%{job | args: Map.put(job.args, "chunk", resp)})

      # Cycle was detected, no point in continuing.
      :discard ->
        {:discard, "Cycle detected. Discarding."}

      :snooze ->
        {:snooze, resp}
    end
  end

  defp log_completed!(repo, worker) do
    {1, _} =
      repo.insert_all("monarch_jobs", [
        %{
          inserted_at:
            DateTime.utc_now()
            |> DateTime.to_naive()
            |> NaiveDateTime.truncate(:second),
          name: to_string(worker)
        }
      ])

    :ok
  end

  defp maybe_transaction(worker, repo, lambda) do
    if function_exported?(worker, :transaction?, 0) && worker.transaction?() do
      repo.transaction(lambda)
    else
      {:ok, lambda.()}
    end
  end

  defp ensure_worker_loaded(worker, attempt) when attempt <= @max_load_attempts do
    case Code.ensure_loaded(worker) do
      {:module, ^worker} ->
        :ok

      {:error, :nofile} ->
        # Module doesn't exist - this can be caused by a race condition where
        # the module is not yet compiled but the job has been enqueued and picked
        # up. We will retry with an exponential backoff up to max attempts.
        delay = calculate_delay(attempt)
        {:retry, attempt + 1, delay}

      {:error, _reason} ->
        # Module exists but failed to load - retry with backoff
        delay = calculate_delay(attempt)
        {:retry, attempt + 1, delay}
    end
  end

  defp ensure_worker_loaded(_worker, attempt) when attempt > @max_load_attempts do
    {:error, :max_retries_exceeded}
  end

  defp calculate_delay(attempt) do
    # Exponential backoff: 5s, 10s, 20s, 40s, 80s
    @base_delay * :math.pow(2, attempt - 1) |> round()
  end
end
