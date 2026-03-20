defmodule Monarch.Migrations do
  @moduledoc """
  This defines the migration for creating the necessary table `monarch_jobs` to use the Monarch behaviour.
  """

  use Ecto.Migration

  @spec up(Keyword.t()) :: :ok
  def up(version: 1) do
    create table(:monarch_jobs, primary_key: false) do
      add(:id, :bigserial, primary_key: true)
      add(:name, :string, null: false)
      add(:inserted_at, :utc_datetime, null: false)
    end

    create(unique_index(:monarch_jobs, [:name]))
    create(index(:monarch_jobs, [:inserted_at]))

    :ok
  end

  @spec down(Keyword.t()) :: :ok
  def down(version: 1) do
    drop(table(:monarch_jobs))

    :ok
  end
end
