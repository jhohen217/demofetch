import asyncio
import discord
from discord.ext import commands
from pathlib import Path
from typing import Optional

from CS2DemoRenderer.src.renderer import DemoRenderer
from CS2DemoRenderer.src.config import Config

class CS2RenderCommands(commands.Cog):
    def __init__(self, bot):
        self.bot = bot
        self.renderer: Optional[DemoRenderer] = None
        
    async def cog_load(self):
        """Initialize renderer when cog is loaded."""
        self.renderer = DemoRenderer()
        await self.renderer.initialize()

    async def cog_unload(self):
        """Cleanup renderer when cog is unloaded."""
        if self.renderer:
            await self.renderer.close()

    @commands.command()
    async def cs2render(self, ctx, demo_name: str):
        """
        Render a CS2 demo file using CS2 and HLAE.
        
        Args:
            ctx: Command context
            demo_name: Name of the demo file (without .dem extension)
        """
        try:
            await ctx.send(f"Starting CS2 render for demo: {demo_name}")
            
            # Construct paths
            demo_path = Config.paths['demos'] / f"{demo_name}.dem"
            sequence_path = Config.get_sequence_file_path(demo_name)
            
            if not demo_path.exists():
                await ctx.send(f"Demo file not found: {demo_path}")
                return
                
            # Create default sequence if none exists
            if not sequence_path.exists():
                default_sequence = [
                    {
                        'tick': 0,
                        'cmd': 'mirv_fov 90'
                    },
                    {
                        'tick': 100,
                        'cmd': 'startmovie output/recording raw'
                    },
                    {
                        'tick': -1,  # Last tick
                        'cmd': 'endmovie'
                    }
                ]
                await self.renderer.create_sequence(default_sequence, sequence_path)
            
            # Start rendering
            await ctx.send("Rendering demo in CS2... This may take a while.")
            output_path = await self.renderer.render_demo(demo_path, sequence_path)
            
            await ctx.send(f"CS2 render complete! Video saved to: {output_path}")
            
            # Upload video if it's small enough
            if output_path.stat().st_size < 8_000_000:  # Discord's file size limit
                await ctx.send(file=discord.File(output_path))
            else:
                await ctx.send("Video file is too large to upload directly to Discord.")
                
        except Exception as e:
            await ctx.send(f"Error rendering demo in CS2: {e}")

    @commands.command()
    async def cs2sequence(self, ctx, demo_name: str, *, commands: str):
        """
        Create a sequence file for CS2 demo rendering.
        
        Args:
            ctx: Command context
            demo_name: Name of the demo file (without .dem extension)
            commands: Commands in format: "tick:command; tick:command; ..."
            
        Example:
            !cs2sequence mydemo "0:mirv_fov 90; 100:startmovie output/clip raw; 500:endmovie"
        """
        try:
            # Parse commands string into sequence
            actions = []
            for cmd in commands.split(';'):
                if ':' not in cmd:
                    continue
                    
                tick, command = cmd.strip().split(':', 1)
                actions.append({
                    'tick': int(tick),
                    'cmd': command.strip()
                })
            
            sequence_path = Config.get_sequence_file_path(demo_name)
            await self.renderer.create_sequence(actions, sequence_path)
            
            await ctx.send(f"Created CS2 sequence file: {sequence_path}")
            
        except Exception as e:
            await ctx.send(f"Error creating CS2 sequence: {e}")

def setup(bot):
    bot.add_cog(CS2RenderCommands(bot))
