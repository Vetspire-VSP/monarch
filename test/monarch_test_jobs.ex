if Mix.env() == :test do
  defmodule MonarchTestEmptyJob do
    @moduledoc """
    A module that implements a monarch job that returns no records to update.
    """

    @behaviour Monarch

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query, do: []

    @impl Monarch
    def update(_), do: :ok
  end

  defmodule MonarchTestSkipJob do
    @moduledoc """
    A module that always skips.
    """

    @behaviour Monarch

    @impl Monarch
    def skip, do: true

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query, do: [1, 2, 3]

    @impl Monarch
    def update(_), do: :ok
  end

  defmodule MonarchTestSnoozeJob do
    @moduledoc """
    A module that always snoozes.
    """

    @behaviour Monarch

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query, do: [1, 2, 3]

    @impl Monarch
    def update(_), do: :ok

    @impl Monarch
    def snooze?, do: 3600
  end

  defmodule MonarchTestCycleJob do
    @moduledoc """
    A module that will never finish.
    """

    @behaviour Monarch

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query, do: [1, 2, 3]

    @impl Monarch
    def update(_), do: :ok
  end

  defmodule MonarchTestDeleteFakeJob do
    @moduledoc """
    A module that implements a monarch job that will delete a record for
    `Elixir.AFakeJob` if one exists.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end

  defmodule MonarchTestAlreadyCompletedJob do
    @moduledoc """
    A module that implements a monarch job that should already be
    completed. This job will delete a record for
    `Elixir.MonarchTestAlreadyCompletedJob` if one exists.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: DateTime.utc_now()

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.MonarchTestAlreadyCompletedJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.MonarchTestAlreadyCompletedJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end

  defmodule MonarchTestManualJob do
    @moduledoc """
    A module that implements a monarch job that should be manually run and should
    not automatically enqueue an Oban job.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: nil

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end

  defmodule MonarchTestScheduledFutureJob do
    @moduledoc """
    A module that implements a monarch job that should be scheduled at the end of the day.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: Timex.end_of_day(DateTime.utc_now())

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end

  defmodule MonarchTestScheduledPastJob do
    @moduledoc """
    A module that implements a monarch job that should be scheduled at the beginning of the day.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: Timex.beginning_of_day(DateTime.utc_now())

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end

  defmodule MonarchTestCustomQueueJob do
    @moduledoc """
    A module that implements a monarch job that specifies a custom Oban queue.
    """

    @behaviour Monarch

    import Ecto.Query

    alias Monarch.Repo

    @impl Monarch
    def queue, do: "custom_queue"

    @impl Monarch
    def skip, do: false

    @impl Monarch
    def scheduled_at, do: Timex.beginning_of_day(DateTime.utc_now())

    @impl Monarch
    def query do
      Repo.all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end

    @impl Monarch
    def update(_) do
      Repo.delete_all(
        from(job in "monarch_jobs",
          where: job.name == "Elixir.AFakeJob",
          select: %{id: job.id, name: job.name, inserted_at: job.inserted_at}
        )
      )
    end
  end
end
