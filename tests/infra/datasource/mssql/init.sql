CREATE TABLE users (
    id INT IDENTITY(1,1) NOT NULL,
    username NVARCHAR(50) NOT NULL,
    password NVARCHAR(50) NOT NULL,
    email NVARCHAR(255),
    PRIMARY KEY(id)
);

INSERT INTO users (username, password, email) VALUES 
('user1', 'password1', 'user1@email.com'),
('user2', 'password2', 'user2@email.com'),
('user3', 'password3', 'user3@email.com'),
('user4', 'password4', 'user4@email.com'),
('user5', 'password5', 'user5@email.com'),
('user6', 'password6', 'user6@email.com'),
('user7', 'password7', 'user7@email.com'),
('user8', 'password8', 'user8@email.com'),
('user9', 'password9', 'user9@email.com'),
('user10', 'password10', 'user10@email.com');
