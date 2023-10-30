# db/seeds.rb
# DB→CSV: `rails generate_csv_from_db`
# CSV→DB: `rails db:seed`

require 'csv'

# CSV作成時とSeed時の年月日の差分日数(generated_before)を加算することで、Seed当日にAppointmentが存在する…といった状況を常に再現する
def offset_dates(hash, generated_before)
  hash[:starts_at] = hash[:starts_at].to_time + generated_before if hash[:starts_at].present?
  hash[:ends_at] = hash[:ends_at].to_time + generated_before if hash[:ends_at].present?

  hash
end

dir = 'db/fixtures/bulk_insert/*.csv'
order_txt = 'db/fixtures/bulk_insert/file_paths_order.txt'
generated_date_txt = 'db/fixtures/bulk_insert/generated_date.txt'

def correct_order_exist?(order_txt, dir)
  File.exist?(order_txt) && File.open(order_txt, 'r').read.split("\n").sort == Dir.glob(dir).sort
end

file_paths = correct_order_exist?(order_txt, dir) ? File.open(order_txt, 'r').read.split("\n") : Dir.glob(dir)
previous_path = ''
inserted_file_paths = []

until file_paths.empty? do
  file_path = file_paths.shift
  csv = CSV.table(file_path)
  next if csv.size.zero?

  begin
    ActiveRecord::Base.transaction do
      model = File.basename(file_path, '.csv').constantize
      generated_date = File.open(generated_date_txt, 'r').read
      generated_before = (Time.current.to_date - generated_date.to_date).to_i.days
      table_name = model.table_name
      model.insert_all! csv.map(&:to_hash).map { |hash| offset_dates(hash, generated_before) }
      # PostgreSQLの場合、idの自動採番をリセットする必要がある
      sql = "SELECT SETVAL ('#{table_name}_id_seq', (SELECT MAX(id) FROM #{table_name}))"
      ActiveRecord::Base.connection.execute(sql)
      p "Finished inserting #{csv.size} records into #{table_name} table!"
      inserted_file_paths << file_path
    end
  rescue ActiveRecord::InvalidForeignKey, ActiveRecord::MismatchedForeignKey => e
    ActiveRecord::Base.connection.execute 'ROLLBACK'
    if file_path != previous_path
      p "Inserting #{file_path} failed due to #{e.message}."
      p "Therefore, it is pushed to the end of file_paths to avoid the error."
      file_paths << file_path
    else
      p "Inserting #{file_path} failed twice in a row. **Inserting #{file_path} was SKIPPED.**"
    end
  end
  previous_path = file_path
end

File.open(order_txt, 'w') do |file|
  file.write(inserted_file_paths.join("\n"))
  p "Finished generating #{order_txt} for future seeding!"
end
