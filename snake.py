import pygame
import random
import sys
from enum import Enum

# Khởi tạo Pygame
pygame.init()

# Hằng số game
WINDOW_WIDTH = 800
WINDOW_HEIGHT = 600
CELL_SIZE = 20
GRID_WIDTH = WINDOW_WIDTH // CELL_SIZE
GRID_HEIGHT = WINDOW_HEIGHT // CELL_SIZE

# Màu sắc
BLACK = (0, 0, 0)
WHITE = (255, 255, 255)
RED = (255, 0, 0)
GREEN = (0, 255, 0)
BLUE = (0, 0, 255)
YELLOW = (255, 255, 0)
PURPLE = (128, 0, 128)

# Tốc độ game
FPS = 10
SPEED_INCREASE = 0.5

class Direction(Enum):
    UP = (0, -1)
    DOWN = (0, 1)
    LEFT = (-1, 0)
    RIGHT = (1, 0)

class Food:
    def __init__(self):
        self.position = (0, 0)
        self.color = RED
        self.respawn()
    
    def respawn(self):
        """Tạo food ở vị trí ngẫu nhiên không trùng với rắn"""
        x = random.randint(0, GRID_WIDTH - 1)
        y = random.randint(0, GRID_HEIGHT - 1)
        self.position = (x, y)
    
    def draw(self, screen):
        """Vẽ food lên màn hình"""
        x, y = self.position
        rect = pygame.Rect(x * CELL_SIZE, y * CELL_SIZE, CELL_SIZE, CELL_SIZE)
        pygame.draw.rect(screen, self.color, rect)
        pygame.draw.rect(screen, WHITE, rect, 2)

class Snake:
    def __init__(self):
        self.reset()
    
    def reset(self):
        """Khởi tạo lại rắn"""
        center_x = GRID_WIDTH // 2
        center_y = GRID_HEIGHT // 2
        self.body = [(center_x, center_y), (center_x - 1, center_y), (center_x - 2, center_y)]
        self.direction = Direction.RIGHT
        self.grow = False
        self.score = 0
    
    def move(self):
        """Di chuyển rắn"""
        head_x, head_y = self.body[0]
        dx, dy = self.direction.value
        new_head = (head_x + dx, head_y + dy)
        
        # Thêm đầu mới
        self.body.insert(0, new_head)
        
        # Nếu không ăn food, xóa đuôi
        if not self.grow:
            self.body.pop()
        else:
            self.grow = False
            self.score += 10
    
    def change_direction(self, new_direction):
        """Thay đổi hướng di chuyển (không cho phép quay ngược lại)"""
        current_dx, current_dy = self.direction.value
        new_dx, new_dy = new_direction.value
        
        # Không cho phép quay ngược lại (trừ khi rắn chỉ có 1 ô)
        if len(self.body) > 1 and (current_dx, current_dy) == (-new_dx, -new_dy):
            return
        
        self.direction = new_direction
    
    def eat_food(self):
        """Rắn ăn food"""
        self.grow = True
    
    def check_collision(self):
        """Kiểm tra va chạm với tường hoặc thân rắn"""
        head_x, head_y = self.body[0]
        
        # Va chạm với tường
        if head_x < 0 or head_x >= GRID_WIDTH or head_y < 0 or head_y >= GRID_HEIGHT:
            return True
        
        # Va chạm với thân rắn
        if (head_x, head_y) in self.body[1:]:
            return True
        
        return False
    
    def draw(self, screen):
        """Vẽ rắn lên màn hình"""
        for i, (x, y) in enumerate(self.body):
            rect = pygame.Rect(x * CELL_SIZE, y * CELL_SIZE, CELL_SIZE, CELL_SIZE)
            
            # Đầu rắn màu khác với thân
            if i == 0:
                pygame.draw.rect(screen, GREEN, rect)
                pygame.draw.rect(screen, BLACK, rect, 3)
            else:
                pygame.draw.rect(screen, BLUE, rect)
                pygame.draw.rect(screen, WHITE, rect, 1)

class Game:
    def __init__(self):
        self.screen = pygame.display.set_mode((WINDOW_WIDTH, WINDOW_HEIGHT))
        pygame.display.set_caption("Snake Game - Rắn Săn Mồi")
        self.clock = pygame.time.Clock()
        self.font = pygame.font.Font(None, 36)
        self.small_font = pygame.font.Font(None, 24)
        self.running = True
        self.game_over = False
        self.paused = False
        
        self.snake = Snake()
        self.food = Food()
        self.current_speed = FPS
    
    def handle_events(self):
        """Xử lý sự kiện bàn phím và chuột"""
        for event in pygame.event.get():
            if event.type == pygame.QUIT:
                self.running = False
            
            elif event.type == pygame.KEYDOWN:
                if event.key == pygame.K_ESCAPE:
                    self.running = False
                
                elif event.key == pygame.K_SPACE:
                    if self.game_over:
                        self.restart_game()
                    else:
                        self.paused = not self.paused
                
                elif event.key == pygame.K_r and self.game_over:
                    self.restart_game()
                
                elif not self.game_over and not self.paused:
                    # Điều khiển di chuyển
                    if event.key == pygame.K_UP or event.key == pygame.K_w:
                        self.snake.change_direction(Direction.UP)
                    elif event.key == pygame.K_DOWN or event.key == pygame.K_s:
                        self.snake.change_direction(Direction.DOWN)
                    elif event.key == pygame.K_LEFT or event.key == pygame.K_a:
                        self.snake.change_direction(Direction.LEFT)
                    elif event.key == pygame.K_RIGHT or event.key == pygame.K_d:
                        self.snake.change_direction(Direction.RIGHT)
    
    def update(self):
        """Cập nhật trạng thái game"""
        if self.game_over or self.paused:
            return
        
        # Di chuyển rắn
        self.snake.move()
        
        # Kiểm tra va chạm
        if self.snake.check_collision():
            self.game_over = True
            return
        
        # Kiểm tra ăn food
        if self.snake.body[0] == self.food.position:
            self.snake.eat_food()
            self.food.respawn()
            
            # Tăng tốc độ khi ăn food
            if self.current_speed < 20:
                self.current_speed += SPEED_INCREASE
    
    def draw(self):
        """Vẽ toàn bộ game"""
        self.screen.fill(BLACK)
        
        # Vẽ grid (tùy chọn)
        self.draw_grid()
        
        # Vẽ rắn và food
        self.snake.draw(self.screen)
        self.food.draw(self.screen)
        
        # Vẽ thông tin game
        self.draw_ui()
        
        # Vẽ trạng thái đặc biệt
        if self.paused:
            self.draw_pause_screen()
        elif self.game_over:
            self.draw_game_over_screen()
        
        pygame.display.flip()
    
    def draw_grid(self):
        """Vẽ lưới (tùy chọn)"""
        for x in range(0, WINDOW_WIDTH, CELL_SIZE):
            pygame.draw.line(self.screen, (50, 50, 50), (x, 0), (x, WINDOW_HEIGHT))
        for y in range(0, WINDOW_HEIGHT, CELL_SIZE):
            pygame.draw.line(self.screen, (50, 50, 50), (0, y), (WINDOW_WIDTH, y))
    
    def draw_ui(self):
        """Vẽ giao diện người dùng"""
        # Hiển thị điểm số
        score_text = self.font.render(f"Điểm: {self.snake.score}", True, WHITE)
        self.screen.blit(score_text, (10, 10))
        
        # Hiển thị độ dài rắn
        length_text = self.small_font.render(f"Độ dài: {len(self.snake.body)}", True, WHITE)
        self.screen.blit(length_text, (10, 50))
        
        # Hiển thị tốc độ
        speed_text = self.small_font.render(f"Tốc độ: {self.current_speed:.1f}", True, WHITE)
        self.screen.blit(speed_text, (10, 80))
        
        # Hiển thị hướng dẫn
        if not self.game_over:
            controls = [
                "WASD hoặc Arrow Keys: Di chuyển",
                "Space: Tạm dừng/Tiếp tục",
                "ESC: Thoát"
            ]
            
            for i, control in enumerate(controls):
                control_text = self.small_font.render(control, True, (200, 200, 200))
                self.screen.blit(control_text, (WINDOW_WIDTH - 250, 10 + i * 25))
    
    def draw_pause_screen(self):
        """Vẽ màn hình tạm dừng"""
        overlay = pygame.Surface((WINDOW_WIDTH, WINDOW_HEIGHT))
        overlay.set_alpha(128)
        overlay.fill(BLACK)
        self.screen.blit(overlay, (0, 0))
        
        pause_text = self.font.render("TẠM DỪNG", True, YELLOW)
        text_rect = pause_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2))
        self.screen.blit(pause_text, text_rect)
        
        continue_text = self.small_font.render("Nhấn Space để tiếp tục", True, WHITE)
        continue_rect = continue_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2 + 50))
        self.screen.blit(continue_text, continue_rect)
    
    def draw_game_over_screen(self):
        """Vẽ màn hình game over"""
        overlay = pygame.Surface((WINDOW_WIDTH, WINDOW_HEIGHT))
        overlay.set_alpha(128)
        overlay.fill(BLACK)
        self.screen.blit(overlay, (0, 0))
        
        game_over_text = self.font.render("GAME OVER", True, RED)
        game_over_rect = game_over_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2 - 50))
        self.screen.blit(game_over_text, game_over_rect)
        
        final_score_text = self.font.render(f"Điểm cuối: {self.snake.score}", True, YELLOW)
        score_rect = final_score_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2))
        self.screen.blit(final_score_text, score_rect)
        
        final_length_text = self.small_font.render(f"Độ dài cuối: {len(self.snake.body)}", True, WHITE)
        length_rect = final_length_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2 + 30))
        self.screen.blit(final_length_text, length_rect)
        
        restart_text = self.small_font.render("Nhấn R để chơi lại hoặc ESC để thoát", True, WHITE)
        restart_rect = restart_text.get_rect(center=(WINDOW_WIDTH // 2, WINDOW_HEIGHT // 2 + 70))
        self.screen.blit(restart_text, restart_rect)
    
    def restart_game(self):
        """Khởi động lại game"""
        self.snake.reset()
        self.food.respawn()
        self.game_over = False
        self.current_speed = FPS
    
    def run(self):
        """Vòng lặp chính của game"""
        while self.running:
            self.handle_events()
            self.update()
            self.draw()
            self.clock.tick(self.current_speed)
        
        pygame.quit()
        sys.exit()

def check_requirements():
    """Kiểm tra yêu cầu hệ thống"""
    try:
        import pygame
        return True
    except ImportError:
        print("Lỗi: Pygame chưa được cài đặt!")
        print("Để cài đặt pygame, hãy chạy lệnh:")
        print("pip install pygame")
        return False

def main():
    """Hàm chính"""
    print("🐍 Chào mừng đến với trò chơi Snake!")
    print("================================")
    print("🎮 Hướng dẫn:")
    print("   - Sử dụng WASD hoặc phím mũi tên để di chuyển")
    print("   - Ăn thức ăn (đỏ) để tăng điểm và độ dài")
    print("   - Tránh va chạm với tường và thân rắn")
    print("   - Nhấn Space để tạm dừng")
    print("   - Nhấn ESC để thoát")
    print("   - Khi thua, nhấn R để chơi lại")
    print("================================")
    print("Đang khởi động game...")
    
    if not check_requirements():
        return
    
    game = Game()
    game.run()

if __name__ == "__main__":
    main()