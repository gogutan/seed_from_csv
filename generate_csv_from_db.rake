# lib/tasks/generate_csv_from_db.rake
require 'csv'

desc 'for generating csv from db'
task :generate_csv_from_db => :environment do |_task, args|
  def generate_csv(model_name)
    model = model_name.constantize
    results = model.pluck(*model.column_names)
    return 0 if results.size.zero?

    data = CSV.generate do |csv|
      csv << model.column_names
      results.each { |result| csv << result }
    end
    file_path = "db/fixtures/bulk_insert/#{model_name}.csv"
    File.open(file_path, 'w') do |file|
      file.write(data)
      p "Finished generating #{file_path}(#{results.size} rows) from DB!"
    end
    return results.size
  end

  total_count = 0
  ActiveRecord::Base.connection
                    .tables
                    .reject{ |t| t.in? %w(schema_migrations ar_internal_metadata) }
                    .each { |t| total_count += generate_csv(t.classify) }
  p "Total row count: #{total_count}"

  File.open("db/fixtures/bulk_insert/generated_date.txt", 'w') do |file|
    generated_date = Time.current.to_s
    file.write(generated_date)
    p "Saved generated_date: #{generated_date} to db/fixtures/bulk_insert/generated_date.txt!"
  end
end
