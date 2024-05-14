CREATE TABLE users (
     id SERIAL PRIMARY KEY,
     username VARCHAR(255) NOT NULL,
     email VARCHAR(255) NOT NULL,
     created_at TIMESTAMP NOT NULL
 );

 INSERT INTO users (username, email, created_at) VALUES
 ('john_doe', 'john@example.com', NOW()),
 ('jane_smith', 'jane@example.com', NOW()),
 ('alex_jones', 'alex@example.com', NOW()); 