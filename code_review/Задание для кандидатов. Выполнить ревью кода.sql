-- 1. Параметры процедуры пишутся в скобках
create procedure syn.usp_ImportFileCustomerSeasonal 
	@ID_Record int
 -- 2. Ключевые слова, названия системных функций и все операторы пишутся в нижнем регистре
AS
set nocount on
begin
	-- 3. Все переменные задаются в одном объявлении
	declare @RowCount int = (select count(*) from syn.SA_CustomerSeasonal) 
	declare @ErrorMessage varchar(max)

-- Проверка на корректность загрузки
	if not exists (
	-- 4. В условных операторах весь блок кода смещается на 1 отступ
	select 1 
	-- 5. При наименовании алиаса использовать первые заглавные буквы каждого слова в названии объекта, которому дают алиас
	from syn.ImportFile as f 
	where f.ID = @ID_Record
		and f.FlagLoaded = cast(1 as bit)
	)
		begin
			set @ErrorMessage = 'Ошибка при загрузке файла, проверьте корректность данных'

			raiserror(@ErrorMessage, 3, 1)
			-- 6. Нехватает пустой строки, пустыми строками отделяются разные логические блоки кода
			return 
		end

	--Чтение из слоя временных данных
	select
		c.ID as ID_dbo_Customer
		,cst.ID as ID_CustomerSystemType
		,s.ID as ID_Season
		,cast(cs.DateBegin as date) as DateBegin
		,cast(cs.DateEnd as date) as DateEnd
		,c_dist.ID as ID_dbo_CustomerDistributor
		,cast(isnull(cs.FlagActive, 0) as bit) as FlagActive
	into #CustomerSeasonal
	--7. Алиас обязателен для объекта и задаётся с помощью ключевого слова "as"
	from syn.SA_CustomerSeasonal cs 
		-- 8. Все виды join-ов указываются явно
		join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer 
			and c.ID_mapping_DataSource = 1
		join dbo.Season as s on s.Name = cs.Season
		join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor
			-- 9. Ошибка, псевдонима "cd." не существует
			and cd.ID_mapping_DataSource = 1 
		-- 10. Сперва указывается поле присоединяемой таблицы
		join syn.CustomerSystemType as cst on cs.CustomerSystemType = cst.Name 
	where try_cast(cs.DateBegin as date) is not null
		and try_cast(cs.DateEnd as date) is not null
		and try_cast(isnull(cs.FlagActive, 0) as bit) is not null

	-- Определяем некорректные записи
	-- Добавляем причину, по которой запись считается некорректной
	select
		cs.*
		,case
			-- 11. При написании конструкции с "case", необходимо, чтобы "when" был под "case" с 1 отступом, "then" с 2 отступами
			when c.ID is null then 'UID клиента отсутствует в справочнике "Клиент"'
			when c_dist.ID is null then 'UID дистрибьютора отсутствует в справочнике "Клиент"'
			when s.ID is null then 'Сезон отсутствует в справочнике "Сезон"'
			when cst.ID is null then 'Тип клиента отсутствует в справочнике "Тип клиента"'
			when try_cast(cs.DateBegin as date) is null then 'Невозможно определить Дату начала'
			when try_cast(cs.DateEnd as date) is null then 'Невозможно определить Дату окончания'
			when try_cast(isnull(cs.FlagActive, 0) as bit) is null then 'Невозможно определить Активность'
		end as Reason
	into #BadInsertedRows
	from syn.SA_CustomerSeasonal as cs
	-- 12. "join"-ы пишутся с одним отступом
	left join dbo.Customer as c on c.UID_DS = cs.UID_DS_Customer
		and c.ID_mapping_DataSource = 1
	-- 13. Дополнительные условия переносятся на следующую строку с 1 отступом
	left join dbo.Customer as c_dist on c_dist.UID_DS = cs.UID_DS_CustomerDistributor and c_dist.ID_mapping_DataSource = 1
	left join dbo.Season as s on s.Name = cs.Season
	left join syn.CustomerSystemType as cst on cst.Name = cs.CustomerSystemType
	-- 14. Ошибка, псевдонима "cс." не существует
	where cc.ID is null
		-- 15. Ошибка,  псевдонима "cd." не существует, та же самая, что и в комментарии №9
		or cd.ID is null
		or s.ID is null
		or cst.ID is null
		or try_cast(cs.DateBegin as date) is null
		or try_cast(cs.DateEnd as date) is null
		or try_cast(isnull(cs.FlagActive, 0) as bit) is null

	-- Обработка данных из файла
	/*
		16. Перед названием таблицы, в которую осуществляется "merge", "into" не указывается
		17. Используется алиас "t"(от target) для объекта, в который осуществляется "merge"
	*/
	merge into syn.CustomerSeasonal as cs
	using (
		select
			cs_temp.ID_dbo_Customer
			,cs_temp.ID_CustomerSystemType
			,cs_temp.ID_Season
			,cs_temp.DateBegin
			,cs_temp.DateEnd
			,cs_temp.ID_dbo_CustomerDistributor
			,cs_temp.FlagActive
		from #CustomerSeasonal as cs_temp
	) as s on s.ID_dbo_Customer = cs.ID_dbo_Customer
		and s.ID_Season = cs.ID_Season
		and s.DateBegin = cs.DateBegin
	-- 18. Все дополнительные условия остаются на строке с "when"
	when matched 
		/*
			19. "then" записывается на одной строке с "when", независимо от наличия дополнительных условий
			20. Ошибка, псевдонима "t." не существует, используется "cs."
		*/
		and t.ID_CustomerSystemType <> s.ID_CustomerSystemType then
		update
		set
			ID_CustomerSystemType = s.ID_CustomerSystemType
			,DateEnd = s.DateEnd
			,ID_dbo_CustomerDistributor = s.ID_dbo_CustomerDistributor
			,FlagActive = s.FlagActive
	when not matched then
		insert (ID_dbo_Customer, ID_CustomerSystemType, ID_Season, DateBegin, DateEnd, ID_dbo_CustomerDistributor, FlagActive)
		values (s.ID_dbo_Customer, s.ID_CustomerSystemType, s.ID_Season, s.DateBegin, s.DateEnd, s.ID_dbo_CustomerDistributor, s.FlagActive)
	-- 21. ";" ставится в конце последней строки конструкции "merge"
	;

	-- Информационное сообщение
	begin
		select @ErrorMessage = concat('Обработано строк: ', @RowCount)

		raiserror(@ErrorMessage, 1, 1)

		-- Формирование таблицы для отчетности
		select top 100
			Season as 'Сезон'
			,UID_DS_Customer as 'UID Клиента'
			,Customer as 'Клиент'
			,CustomerSystemType as 'Тип клиента'
			,UID_DS_CustomerDistributor as 'UID Дистрибьютора'
			,CustomerDistributor as 'Дистрибьютор'
			,isnull(format(try_cast(DateBegin as date), 'dd.MM.yyyy', 'ru-RU'), DateBegin) as 'Дата начала'
			,isnull(format(try_cast(DateEnd as date), 'dd.MM.yyyy', 'ru-RU'), DateEnd) as 'Дата окончания'
			,FlagActive as 'Активность'
			,Reason as 'Причина'
		from #BadInsertedRows

		return
	end
-- 22. Тут не нужна пустая строка
end
