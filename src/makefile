# Thank you, Gemini! ^_^

# The compiler to use
CXX = g++

# The flags (c++17 and pthread are required for your engine)
CXXFLAGS = -std=c++17 -pthread -Wall -O0 -g

# The name of your database executable
TARGET = imdb_server

# The default rule when you type 'make'
all:
	@echo "Compiling all files into $(TARGET)..."
	$(CXX) $(CXXFLAGS) *.cpp -o $(TARGET)
	@echo "Done!"

# Rule to clean up the executable when you type 'make clean'
clean:
	rm -f $(TARGET)