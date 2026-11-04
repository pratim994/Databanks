

create table if not exists customers (
	id serial primary key,
	first_name varchar(50) not null,
	last_name varchar(50) not null,
	email varchar(50) unique not null,
	created_at timestamp with time zone default now()
);


create table if not exists accounts (
	id serial primary key,
	customer_id INT not null references customer(id) on delete cascade,
	account_type varchar(50) not null,
	balance numeric(18,2) not null default 0 check (balance >= 0),
	currency char(3) not null default 'usd',
	created_at timestamp with time zone default now()
);

create table if not exists transactions (
	id bigserial primary key,
	account_id int not null references accounts(id) on delete cascade,
	txn_type varchar(50) not null, --DEPOSIT | WITHDRAWAL | TRANSFER 
	amount numeric(18,2) not null check (amount > 0),
	related_account_id int null, -- for transfer
	status varchar(20) not null default 'COMPLETED',
	created_at timestamp with time zone default now()
);


create index if not exists idx_transactions_account_created on transactions(account_id, created_at);
